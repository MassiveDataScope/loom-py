"""Result codecs for repository reads marked with ``@cache_query``.

A cached custom read has to answer with the same type whether the value came
from the repository or from the cache. The stored payload is builtins, so
without a codec the first call returns whatever the method built and every
later call returns the decoded payload — a struct on the miss, a ``dict`` on
the hit.

The codec is derived from the method's declared return annotation and applied
on both paths, so the two are indistinguishable and a value that does not
survive the round trip fails on the very first call. The supported grammar is
a :class:`msgspec.Struct`, a scalar, or a ``list``, ``tuple`` or optional of
those; ``tuple`` is restored as a tuple, which builtins conversion flattens
into a list. An annotation outside the grammar has no codec, and
:class:`~loom.core.cache.repository.CachedRepository` refuses to wrap a
repository with such a read.

A coroutine marked with ``@cache_call`` uses the same machinery through
:func:`build_call_codec`, which extends the grammar with pydantic types behind
a lazily loaded adapter and refuses, rather than passes through, an annotation
it cannot describe.

The lenient builtins rendering the wrapper uses for its keys and entity
payloads lives here too.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from datetime import date, datetime, time
from decimal import Decimal
from types import GenericAlias, NoneType, UnionType
from typing import (
    Any,
    NamedTuple,
    Protocol,
    TypeVar,
    Union,
    get_args,
    get_origin,
    is_typeddict,
)
from uuid import UUID

import msgspec

from loom.core.model import LoomType, is_pydantic_model, loom_type_of, pydantic_type
from loom.core.model.introspection import resolve_type_hints

_SCALAR_TYPES: frozenset[type] = frozenset(
    {bool, int, float, str, bytes, UUID, datetime, date, time, Decimal}
)
_UNION_ORIGINS: frozenset[Any] = frozenset({Union, UnionType})


def to_payload(value: Any) -> Any:
    """Render *value* as builtins, leaving what it cannot describe untouched.

    Unlike :func:`msgspec.to_builtins` this never raises: a value outside the
    struct, pydantic model, sequence and mapping shapes it knows is returned
    as it is, and the backend serializer decides whether it can store it.

    Args:
        value: Value on its way to the cache backend.

    Returns:
        The builtins rendering of *value*.
    """
    if isinstance(value, msgspec.Struct):
        return msgspec.to_builtins(value)
    if is_pydantic_model(type(value)):
        return loom_type_of(type(value)).to_builtins(value)
    if isinstance(value, list | tuple):
        return [to_payload(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_payload(item) for key, item in value.items()}
    return value


class EncodedResult(NamedTuple):
    """A freshly loaded result, split into what to store and what to return.

    Attributes:
        payload: Builtins form written to the cache backend.
        value: Value handed back to the caller of the cached read.
    """

    payload: Any
    value: Any


class ResultCodec(Protocol):
    """Translation between a cached read's payload and its declared type."""

    def encode(self, result: Any) -> EncodedResult:
        """Split a freshly loaded *result* into its payload and return value."""
        ...

    def decode(self, payload: Any) -> Any:
        """Rebuild the return value from a *payload* read out of the cache."""
        ...


class _LoomTypeResultCodec:
    """Codec that converts both paths through a single :class:`LoomType`.

    Shared by the msgspec grammar :func:`build_result_codec` implements and
    by the pydantic fallback :func:`build_call_codec` extends it with: both
    resolve to one already-built :class:`LoomType` and differ only in which
    factory built it.
    """

    __slots__ = ("_loom_type",)

    def __init__(self, loom_type: LoomType) -> None:
        """Bind the codec to an already built boundary type.

        Args:
            loom_type: Boundary type compiled from the cached call's or
                read's resolved return annotation.
        """
        self._loom_type = loom_type

    def encode(self, result: Any) -> EncodedResult:
        """Render *result* as builtins and decode it back.

        Returning the decoded value rather than *result* is what makes a miss
        and a hit indistinguishable, and it surfaces a result that cannot be
        stored on the first call instead of the second.

        Raises:
            BoundaryValidationError: The rendered result does not match the
                declared return type.
        """
        payload = self._loom_type.to_builtins(result)
        return EncodedResult(payload, self.decode(payload))

    def decode(self, payload: Any) -> Any:
        """Convert *payload* to the declared return type.

        Raises:
            BoundaryValidationError: The payload does not match the declared
                return type.
        """
        return self._loom_type.from_builtins(payload)


def build_call_codec(func: Callable[..., Any]) -> ResultCodec | None:
    """Build the codec declared by the return annotation of a cached call.

    The msgspec grammar :func:`build_result_codec` implements is tried first;
    a pydantic type falls through to a single ``TypeAdapter``. A mapping is
    refused by decision — parameterised, bare, a ``TypedDict``, or nested
    anywhere inside the annotation — as are ``Any``, a missing annotation, a
    coroutine declared ``-> None`` (which has nothing to cache; ``get_type_hints``
    normalises it to ``NoneType``, so the missing-annotation guard does not
    cover it), an unresolvable forward reference and any pydantic annotation in
    a process that has not imported pydantic: the caller then runs the
    coroutine uncached instead of storing something a hit and a miss would
    disagree about.

    Args:
        func: Coroutine function marked with ``@cache_call``.

    Returns:
        The codec for the declared type, or ``None`` when there is none.
    """
    annotation = resolve_type_hints(func).get("return")
    if annotation is None or annotation is NoneType or annotation is Any:
        return None
    if _mentions_mapping(annotation):
        return None
    return_type = _resolve_return_type(annotation, None)
    if return_type is not None:
        return _LoomTypeResultCodec(loom_type_of(return_type))
    return _pydantic_codec(annotation)


def _mentions_mapping(annotation: Any) -> bool:
    """Return whether *annotation* declares a mapping at any level.

    Only the outermost check is not enough: ``dict[str, Any] | None`` and
    ``list[dict[str, Any]]`` have a union and a list at the top, fall through
    the msgspec grammar, and a ``TypeAdapter`` builds for them happily — so a
    mapping the owner decided is out of the grammar would be cached inside a
    container. The walk descends through every type argument.

    Args:
        annotation: Resolved return annotation, or one of its arguments.

    Returns:
        Whether a mapping appears anywhere in the annotation.
    """
    if _is_mapping(annotation):
        return True
    return any(_mentions_mapping(argument) for argument in get_args(annotation))


def _is_mapping(annotation: Any) -> bool:
    """Return whether one annotation level declares a mapping, in any of its forms.

    The test is positive rather than a by-product of ``get_origin``: a bare
    ``dict``, a bare ``Mapping`` and a ``TypedDict`` all have no origin and
    would otherwise pass.

    Args:
        annotation: One level of a resolved return annotation.

    Returns:
        Whether this level is a mapping.
    """
    if is_typeddict(annotation):
        return True
    declared = get_origin(annotation) or annotation
    return isinstance(declared, type) and issubclass(declared, Mapping)


def _pydantic_codec(annotation: Any) -> ResultCodec | None:
    """Build a pydantic codec for *annotation*, when pydantic is already loaded.

    Args:
        annotation: Resolved return annotation outside the msgspec grammar.

    Returns:
        The adapter-backed codec, or ``None`` when pydantic is not already
        imported in the process — the guard is ``sys.modules``, not whether
        the package is installed — or cannot describe *annotation*.
    """
    if sys.modules.get("pydantic") is None:
        return None
    try:
        return _LoomTypeResultCodec(pydantic_type(annotation))
    except Exception:
        return None


def build_result_codec(method: Callable[..., Any], *, model: object | None) -> ResultCodec | None:
    """Build the codec declared by the return annotation of *method*.

    The annotation is resolved against the module that defines the method, so
    a string annotation left by ``from __future__ import annotations`` and a
    forward reference resolvable from there both work. A type variable is
    substituted by *model*, mirroring how the wrapper rebuilds an entity read
    from its cached payload.

    Args:
        method: Function decorated with ``@cache_query``, taken from the class
            dictionary rather than from an instance.
        model: Model of the repository owning the method, used to resolve a
            type variable in the annotation.

    Returns:
        The codec for the declared type, or ``None`` when the annotation is
        missing, unresolvable or outside the supported grammar.
    """
    annotation = resolve_type_hints(method).get("return")
    if annotation is None:
        return None
    return_type = _resolve_return_type(annotation, model)
    if return_type is None:
        return None
    return _LoomTypeResultCodec(loom_type_of(return_type))


def _resolve_return_type(annotation: Any, model: object | None) -> Any | None:
    optional = _resolve_optional(annotation, model)
    if optional is not None:
        return optional
    return _resolve_concrete(annotation, model)


def _resolve_optional(annotation: Any, model: object | None) -> Any | None:
    if get_origin(annotation) not in _UNION_ORIGINS:
        return None
    members = get_args(annotation)
    if len(members) != 2 or NoneType not in members:
        return None
    declared = next(member for member in members if member is not NoneType)
    inner = _resolve_concrete(declared, model)
    if inner is None:
        return None
    return inner | None


def _resolve_concrete(annotation: Any, model: object | None) -> Any | None:
    origin = get_origin(annotation)
    if origin is list:
        return _resolve_list(get_args(annotation), model)
    if origin is tuple:
        return _resolve_tuple(get_args(annotation), model)
    return _resolve_element(annotation, model)


def _resolve_list(args: tuple[Any, ...], model: object | None) -> Any | None:
    if len(args) != 1:
        return None
    element = _resolve_element(args[0], model)
    if element is None:
        return None
    return GenericAlias(list, (element,))


def _resolve_tuple(args: tuple[Any, ...], model: object | None) -> Any | None:
    if not args:
        return None
    if len(args) == 2 and args[1] is Ellipsis:
        element = _resolve_element(args[0], model)
        return None if element is None else GenericAlias(tuple, (element, Ellipsis))
    elements = [_resolve_element(arg, model) for arg in args]
    if any(element is None for element in elements):
        return None
    return GenericAlias(tuple, tuple(elements))


def _resolve_element(annotation: Any, model: object | None) -> type | None:
    declared = model if isinstance(annotation, TypeVar) else annotation
    if not isinstance(declared, type):
        return None
    if issubclass(declared, msgspec.Struct):
        return declared
    if declared in _SCALAR_TYPES:
        return declared
    return None
