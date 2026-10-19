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
import math
import re
import sys
import typing
from collections.abc import Callable, Mapping
from typing import Any, Literal, Protocol

import msgspec

from loom.core.errors.codes import ErrorCode
from loom.core.errors.errors import LoomError
from loom.core.model._json_schema import DEFS, definition, without_null

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

    def from_query(self, params: Mapping[str, str], /) -> Any:
        """Validate query-string values strictly and build the typed value.

        Every value arrives as text. A number field takes the JSON number
        grammar (no ``+``, no leading zero, no bare ``.``) and a finite value;
        an integer field takes the integer subset of it, within
        ``[-2**63, 2**64)``; a boolean field takes ``true`` or ``false``.
        Every other field validates the text strictly, so ``"null"`` is not
        ``None``, and a date or time field refuses a number instead of reading
        it as a timestamp.

        Raises:
            BoundaryValidationError: ``params`` does not satisfy the type.
        """
        ...


class BoundaryValidationError(LoomError, ValueError):
    """A boundary value failed validation.

    Raised by every :class:`LoomType` implementation with the library error as
    ``__cause__``. The pydantic message is built field by field and omits the
    input and any documentation URL, and its tag, UUID and bytes-encoding
    reasons drop the detail pydantic derives from the input; the msgspec
    message is forwarded as one line, without the value msgspec echoes for an
    invalid enum value or tag. A message raised by the type's own validator
    keeps its text. It is also a
    :class:`ValueError`, so an ``except ValueError`` that used to catch
    ``msgspec.ValidationError`` or ``pydantic.ValidationError`` keeps
    catching it.

    Args:
        message: One-line description of the failure.
        violations: ``(field, message)`` pairs, one per failed field. Empty
            when the caller has no per-field breakdown to offer.
        code: Defaults to ``boundary_validation``; a transport may re-raise
            under its own code.
    """

    def __init__(
        self,
        message: str,
        violations: tuple[tuple[str, str], ...] = (),
        *,
        code: str = ErrorCode.BOUNDARY_VALIDATION,
    ) -> None:
        super().__init__(message, code=code)
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
            raise _msgspec_error(exc) from exc

    def to_builtins(self, obj: Any, /) -> Any:
        return msgspec.to_builtins(obj)

    @functools.cached_property
    def _coercers(self) -> Mapping[str, Callable[[str], Any]]:
        return _query_coercers(self.schema())

    def from_builtins(self, payload: Any, /) -> Any:
        try:
            return msgspec.convert(payload, self.type)
        except msgspec.ValidationError as exc:
            raise _msgspec_error(exc) from exc

    def from_query(self, params: Mapping[str, str], /) -> Any:
        return self.from_builtins(_coerce_query(params, self._coercers))


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

    @functools.cached_property
    def _coercers(self) -> Mapping[str, Callable[[str], Any]]:
        return _query_coercers(self.schema())

    def from_query(self, params: Mapping[str, str], /) -> Any:
        return self.decode_json(msgspec.json.encode(_coerce_query(params, self._coercers)))


_INTEGER = re.compile(r"-?(?:0|[1-9]\d*)", re.ASCII)
_FRACTION_EXPONENT = re.compile(r"(?:\.\d+)?(?:[eE][+-]?\d+)?", re.ASCII)
_LOOSE_NUMBER = re.compile(r"[+-]?[\d.]+(?:[eE][+-]?\d+)?", re.ASCII)
_INT_RANGE = range(-(2**63), 2**64)
_INT_MAX_LENGTH = len(str(-(2**63)))
_BOOLEANS = {"true": True, "false": False}
_TEMPORAL_FORMATS = frozenset({"date-time", "date", "time"})
_NOT_RFC3339 = "Expected an RFC 3339 value, got a number"


def _to_int(text: str) -> Any:
    if len(text) > _INT_MAX_LENGTH or not _INTEGER.fullmatch(text):
        return text
    number = int(text)
    return number if number in _INT_RANGE else text


def _to_float(text: str) -> Any:
    integer = _INTEGER.match(text)
    if integer is None or not _FRACTION_EXPONENT.fullmatch(text, integer.end()):
        return text
    number = float(text)
    return number if math.isfinite(number) else text


def _to_bool(text: str) -> Any:
    return _BOOLEANS.get(text, text)


def _refuse_number(text: str) -> str:
    if _LOOSE_NUMBER.fullmatch(text):
        raise ValueError(_NOT_RFC3339)
    return text


_COERCERS: dict[str, Callable[[str], Any]] = {
    "integer": _to_int,
    "number": _to_float,
    "boolean": _to_bool,
}


def _query_coercers(schema: Mapping[str, Any]) -> dict[str, Callable[[str], Any]]:
    definitions = schema.get(DEFS, {})
    root = definition(schema, definitions)
    coercers: dict[str, Callable[[str], Any]] = {}
    for name, prop in root.get("properties", {}).items():
        coerce = _coercer(definition(without_null(prop), definitions))
        if coerce is not None:
            coercers[name] = coerce
    return coercers


def _coercer(target: Mapping[str, Any]) -> Callable[[str], Any] | None:
    if target.get("format") in _TEMPORAL_FORMATS:
        return _refuse_number
    values = target.get("enum") or ()
    if values and all(type(value) is int for value in values):
        return _to_int
    return _COERCERS.get(target.get("type", ""))


def _coerce_query(
    params: Mapping[str, str], coercers: Mapping[str, Callable[[str], Any]]
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for name, text in params.items():
        coerce = coercers.get(name)
        try:
            payload[name] = text if coerce is None else coerce(text)
        except ValueError as exc:
            raise BoundaryValidationError(f"{exc} - at `$.{name}`", ((name, str(exc)),)) from exc
    return payload


_PYDANTIC_ECHOING_REASONS = {
    "union_tag_invalid": "Input tag does not match any of the expected tags",
    "uuid_parsing": "Input should be a valid UUID",
    "bytes_invalid_encoding": "Data should be valid in its declared encoding",
}


def _pydantic_violations(exc: Any) -> tuple[tuple[str, str], ...]:
    return tuple(
        (
            "body" if not error["loc"] else ".".join(map(str, error["loc"])),
            _PYDANTIC_ECHOING_REASONS.get(error["type"], str(error["msg"])),
        )
        for error in exc.errors(include_url=False, include_input=False)
    )


_MSGSPEC_ECHOING_REASONS = ("Invalid enum value", "Invalid value")
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


def _msgspec_error(exc: msgspec.DecodeError) -> BoundaryValidationError:
    """Build the loom error from *exc*, dropping any value msgspec echoes from the input."""
    text = str(exc)
    message, _ = _msgspec_at_path(text)
    reason = next(
        (reason for reason in _MSGSPEC_ECHOING_REASONS if message.startswith(f"{reason} ")),
        message,
    )
    text = reason + text.removeprefix(message)
    return BoundaryValidationError(text, (_msgspec_violation(text),))


def _msgspec_violation(text: str) -> tuple[str, str]:
    """Parse a msgspec validation message into a ``(field, message)`` violation.

    Args:
        text: The one-line message a msgspec ``ValidationError`` carries.

    Returns:
        The violation's field (``"body"`` when the message names none) and
        the message with any trailing ``- at ...`` location stripped.
    """
    message, path = _msgspec_at_path(text)
    if path is not None:
        path = path.replace("[...]", "")
        path = _MSGSPEC_ARRAY_INDEX.sub(r".\1", path)
        path = path.removeprefix(".")

    field_match = _MSGSPEC_FIELD.search(message)
    named_field = field_match.group(1) if field_match else None

    field = ".".join(part for part in (path, named_field) if part) or "body"
    return field, message
