"""Loom-owned boundary type: one contract over msgspec and pydantic.

A :class:`LoomType` is what a boundary carries once compiled: it validates
JSON strictly, builds the typed value in the same pass and turns a value
back into JSON-mode builtins. Agent output, agent state, the cache, MCP
results and REST are all served this way. The two implementations
are private to this module, so no other module calls ``msgspec.json.Decoder``
or ``pydantic.TypeAdapter`` on a boundary value; the library is decided once,
at compile time, and every consumer that needs to branch on it afterwards
reads the ``library`` tag it was constructed with.

Pydantic stays optional: the factory only looks at ``sys.modules`` and imports
``pydantic`` locally, so a process that never imported it keeps working with
msgspec alone. That is also why the pydantic implementation receives its model
class untyped: this module never names ``pydantic.BaseModel``, and loom's own
``loom.core.model.BaseModel`` lives next door.
"""

from __future__ import annotations

import functools
import re
import sys
import typing
from collections.abc import Callable, Mapping
from typing import Any, Literal, Protocol

import msgspec

from loom.core.errors.codes import ErrorCode
from loom.core.errors.errors import BODY_FIELD, LoomError

_LAX_TYPE_REASON = (
    "the type must reject unknown fields: forbid_unknown_fields=True or "
    "model_config['extra'] == 'forbid' (invariant 5)"
)
_NOT_A_MODEL_REASON = "only msgspec.Struct or pydantic.BaseModel subclasses are supported"


class LoomType(Protocol):
    """Boundary contract a compiled type exposes to its consumers.

    Attributes:
        type: The declared type or a compiler-generated annotation, kept for
            introspection.
        library: Closed tag naming the implementation behind the value.
    """

    type: Any
    library: Literal["msgspec", "pydantic"]

    def schema(self) -> Mapping[str, Any]:
        """Return the JSON Schema of the wrapped type."""
        ...

    def decode_json(self, data: bytes | str, /) -> Any:
        """Validate ``data`` strictly and build the typed value in one pass.

        Raises:
            BoundaryValidationError: ``data`` is malformed JSON or does not
                satisfy the type.
        """
        ...

    def to_builtins(self, obj: Any, /) -> Any:
        """Turn a typed value into JSON-mode builtins."""
        ...

    def from_builtins(self, payload: Any, /) -> Any:
        """Validate builtins already produced by a trusted source and build the typed value.

        Unlike :meth:`decode_json` this takes Python builtins rather than JSON
        text, and pydantic validates them in its lax mode: it is the entry
        point for a value that already round-tripped through a cache backend
        or an MCP client rather than through raw JSON.

        Raises:
            BoundaryValidationError: ``payload`` does not satisfy the type.
        """
        ...


class BoundaryValidationError(LoomError, ValueError):
    """A boundary value failed validation.

    Raised by every :class:`LoomType` implementation with the library error as
    ``__cause__``. The pydantic message is built field by field and omits the
    input and any documentation URL; the msgspec message is forwarded
    verbatim, and is one line and free of the input already. It is also a
    :class:`ValueError`, so an ``except ValueError`` that used to catch
    ``msgspec.ValidationError`` or ``pydantic.ValidationError`` keeps
    catching it.

    Args:
        message: One-line description of the failure.
        violations: ``(field, message)`` pairs, one per failed field. Empty
            when the caller has no per-field breakdown to offer.
    """

    def __init__(self, message: str, violations: tuple[tuple[str, str], ...] = ()) -> None:
        super().__init__(message, code=ErrorCode.BOUNDARY_VALIDATION)
        self.violations = violations


class UnsupportedBoundaryType(LoomError):
    """A symbol cannot be compiled into a :class:`LoomType`.

    Args:
        reason: Why the symbol is rejected, shared by every boundary's issue.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason, code="boundary_type_unsupported")


def loom_type(symbol: type) -> LoomType:
    """Compile an authored class into a :class:`LoomType`.

    Strictness is a compile-time property of an authored symbol only: a
    ``msgspec.Struct`` must declare ``forbid_unknown_fields=True`` and a
    ``pydantic.BaseModel`` must resolve ``model_config["extra"] == "forbid"``.
    The check happens once, here, before the symbol is handed to
    :func:`loom_type_of`; a compiler-generated annotation built from an
    authored symbol never repeats it.

    Args:
        symbol: A strict ``msgspec.Struct`` or strict ``pydantic.BaseModel``
            subclass.

    Returns:
        The boundary type wrapping ``symbol``.

    Raises:
        UnsupportedBoundaryType: ``symbol`` is neither library's model class or
            does not reject unknown fields.
    """
    if not isinstance(symbol, type):
        raise UnsupportedBoundaryType(_NOT_A_MODEL_REASON)
    if issubclass(symbol, msgspec.Struct) or is_pydantic_model(symbol):
        _require_strict(symbol)
    else:
        raise UnsupportedBoundaryType(_NOT_A_MODEL_REASON)
    return loom_type_of(symbol)


def msgspec_type(annotation: Any) -> LoomType:
    """Wrap a compiler-generated annotation in a msgspec-backed :class:`LoomType`.

    Accepts whatever ``msgspec.json.Decoder`` accepts: a ``defstruct`` result,
    ``dict[str, Any]``, ``list[...]`` or a scalar. Strictness is not checked
    because the annotation is not authored by a user.

    Args:
        annotation: Any annotation ``msgspec.json.Decoder`` can build.

    Returns:
        The boundary type wrapping ``annotation``.

    Raises:
        UnsupportedBoundaryType: ``msgspec.json.Decoder`` cannot build
            ``annotation``.
    """
    try:
        return _MsgspecType(annotation)
    except TypeError as exc:
        raise UnsupportedBoundaryType(str(exc)) from exc


def pydantic_type(annotation: Any) -> LoomType:
    """Wrap any annotation pydantic can describe in a pydantic-backed :class:`LoomType`.

    Covers a ``BaseModel``, a ``RootModel``, a parameterised generic model, a
    ``pydantic.dataclasses`` type and ``list``/``tuple``/union of those — the
    same ``TypeAdapter``-backed implementation the authored ``BaseModel`` case
    in :func:`loom_type` uses. Strictness is not checked because the
    annotation is not necessarily an authored root symbol.

    Args:
        annotation: Any annotation ``pydantic.TypeAdapter`` can build.

    Returns:
        The boundary type wrapping ``annotation``.

    Raises:
        UnsupportedBoundaryType: ``pydantic.TypeAdapter`` cannot build
            ``annotation``.
    """
    try:
        return _PydanticType(annotation)
    except TypeError as exc:
        raise UnsupportedBoundaryType(str(exc)) from exc


def loom_type_of(annotation: Any) -> LoomType:
    """Compile any annotation, authored or compiler-generated, into a :class:`LoomType`.

    The single dispatcher every boundary uses once ``loom_type`` has already
    checked the strictness of an authored root: a ``msgspec.Struct`` class
    resolves to :func:`msgspec_type`, a ``pydantic.BaseModel`` class or an
    annotation that mentions one anywhere inside its ``typing.get_args`` walk
    resolves to :func:`pydantic_type`, an annotation that mentions both
    libraries is rejected, and anything else resolves to :func:`msgspec_type`.

    The result is memoised by hash/equality of the annotation (typing
    generics compare structurally); the memo lives for the process, because
    it is a pure factory: the same annotation always builds the same kind of
    :class:`LoomType`, and building one is not free — it compiles a decoder or
    a ``TypeAdapter``. An annotation that is not hashable (for example
    ``Annotated`` metadata carrying a ``dict``) simply skips the cache.

    Args:
        annotation: Any annotation, authored or compiler-generated.

    Returns:
        The boundary type wrapping ``annotation``.

    Raises:
        UnsupportedBoundaryType: ``annotation`` mentions both a
            ``msgspec.Struct`` and a ``pydantic.BaseModel``.
    """
    try:
        hash(annotation)
    except TypeError:
        return _build_loom_type_of(annotation)
    return _loom_type_of_cached(annotation)


@functools.cache
def _loom_type_of_cached(annotation: Any) -> LoomType:
    return _build_loom_type_of(annotation)


def _build_loom_type_of(annotation: Any) -> LoomType:
    has_struct = _mentions(annotation, _is_struct_class)
    has_model = _mentions(annotation, is_pydantic_model)
    if has_struct and has_model:
        raise UnsupportedBoundaryType(f"{annotation!r} mixes msgspec and pydantic types")
    if has_model:
        return pydantic_type(annotation)
    return msgspec_type(annotation)


def _mentions(annotation: Any, predicate: Callable[[Any], bool]) -> bool:
    if predicate(annotation):
        return True
    return any(_mentions(argument, predicate) for argument in typing.get_args(annotation))


def _is_struct_class(annotation: Any) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, msgspec.Struct)


def _require_strict(symbol: type) -> None:
    if not _is_strict(symbol):
        raise UnsupportedBoundaryType(_LAX_TYPE_REASON)


def _is_strict(symbol: type) -> bool:
    if issubclass(symbol, msgspec.Struct):
        return symbol.__struct_config__.forbid_unknown_fields
    config: Mapping[str, Any] = getattr(symbol, "model_config", {})
    return config.get("extra") == "forbid"


def is_pydantic_model(symbol: Any) -> bool:
    """Report whether *symbol* is a ``pydantic.BaseModel`` subclass.

    Total over any *symbol*: a non-class value answers ``False`` rather than
    raising, so a caller can probe an annotation argument without first
    checking it is a class.

    Args:
        symbol: Value to test.

    Returns:
        ``True`` when *symbol* is a ``pydantic.BaseModel`` subclass and
        pydantic is already imported.
    """
    if not isinstance(symbol, type):
        return False
    pydantic = sys.modules.get("pydantic")
    return pydantic is not None and issubclass(symbol, pydantic.BaseModel)


class _MsgspecType:
    def __init__(self, annotation: Any) -> None:
        self.type: Any = annotation
        self.library: Literal["msgspec", "pydantic"] = "msgspec"
        self._decoder: msgspec.json.Decoder[Any] = msgspec.json.Decoder(annotation)

    def schema(self) -> Mapping[str, Any]:
        return msgspec.json.schema(self.type)

    def decode_json(self, data: bytes | str, /) -> Any:
        try:
            return self._decoder.decode(data)
        except (msgspec.ValidationError, msgspec.DecodeError) as exc:
            text = str(exc)
            raise BoundaryValidationError(text, (_msgspec_violation(text),)) from exc

    def to_builtins(self, obj: Any, /) -> Any:
        return msgspec.to_builtins(obj)

    def from_builtins(self, payload: Any, /) -> Any:
        try:
            return msgspec.convert(payload, self.type)
        except msgspec.ValidationError as exc:
            text = str(exc)
            raise BoundaryValidationError(text, (_msgspec_violation(text),)) from exc


class _PydanticType:
    def __init__(self, symbol: Any) -> None:
        from pydantic import TypeAdapter, ValidationError

        self.type: Any = symbol
        self.library: Literal["msgspec", "pydantic"] = "pydantic"
        self._adapter: TypeAdapter[Any] = TypeAdapter(symbol)
        self._validation_error: type[ValidationError] = ValidationError

    def schema(self) -> Mapping[str, Any]:
        schema: dict[str, Any] = self._adapter.json_schema()
        schema.pop("title", None)
        return schema

    def decode_json(self, data: bytes | str, /) -> Any:
        try:
            return self._adapter.validate_json(data, strict=True)
        except self._validation_error as exc:
            violations = _pydantic_violations(exc)
            message = "; ".join(f"{field}: {msg}" for field, msg in violations)
            raise BoundaryValidationError(message, violations) from exc

    def to_builtins(self, obj: Any, /) -> Any:
        return self._adapter.dump_python(obj, mode="json", by_alias=True)

    def from_builtins(self, payload: Any, /) -> Any:
        try:
            return self._adapter.validate_python(payload)
        except self._validation_error as exc:
            violations = _pydantic_violations(exc)
            message = "; ".join(f"{field}: {msg}" for field, msg in violations)
            raise BoundaryValidationError(message, violations) from exc


def _pydantic_violations(exc: Any) -> tuple[tuple[str, str], ...]:
    return tuple(
        (BODY_FIELD if not error["loc"] else ".".join(map(str, error["loc"])), error["msg"])
        for error in exc.errors(include_url=False, include_input=False)
    )


_MSGSPEC_AT_MARKER = " - at `"
_MSGSPEC_AT_KEY_IN_PREFIX = "key` in `"
_MSGSPEC_FIELD = re.compile(r"(?:missing required|contains unknown) field `([^`]+)`")
_MSGSPEC_ARRAY_INDEX = re.compile(r"\[(\d+)\]")


def _msgspec_at_path(text: str) -> tuple[str, str | None]:
    """Split *text* into its message and trailing ``- at ...`` path, if any.

    Uses plain string operations rather than a backtracking-prone regular
    expression, since the location suffix is a fixed literal shape rather
    than a pattern worth expressing declaratively.
    """
    marker_index = text.rfind(_MSGSPEC_AT_MARKER)
    if marker_index == -1:
        return text, None

    suffix = text[marker_index + len(_MSGSPEC_AT_MARKER) :]
    suffix = suffix.removeprefix(_MSGSPEC_AT_KEY_IN_PREFIX)
    if not suffix.startswith("$") or not suffix.endswith("`"):
        return text, None

    return text[:marker_index], suffix[1:-1]


def _msgspec_violation(text: str) -> tuple[str, str]:
    """Parse a msgspec validation message into a ``(field, message)`` violation.

    Args:
        text: The one-line message a msgspec ``ValidationError`` carries.

    Returns:
        The violation's field (:data:`~loom.core.errors.BODY_FIELD` when the
        message names none) and
        the message with any trailing ``- at ...`` location stripped.
    """
    message, path = _msgspec_at_path(text)
    if path is not None:
        path = path.replace("[...]", "")
        path = _MSGSPEC_ARRAY_INDEX.sub(r".\1", path)
        path = path.removeprefix(".")

    field_match = _MSGSPEC_FIELD.search(message)
    named_field = field_match.group(1) if field_match else None

    field = ".".join(part for part in (path, named_field) if part) or BODY_FIELD
    return field, message
