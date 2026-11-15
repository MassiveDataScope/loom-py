"""Binding of ``@cache_call`` coroutines to a configured cache.

The decorator declares and the composition root binds: a marked coroutine is
imported and tested with no configuration at all, and the ``CachedCalls`` a
container holds — built once at boot from the same ``cache:`` section the
repositories read — is what turns it into a cached call.

A cached call is a TTL cache with **no invalidation**: loom cannot know what a
coroutine depends on, so nothing tags it and nothing bumps it. It expires, or
the caller bumps ``version``.

The key is the arguments and nothing else. That is what makes an answer
shareable between two callers and between two instances of a toolset class, and
it is why a cached coroutine must be a pure function of its arguments: ambient
identity never reaches the key, and the load runs detached in its own task.
"""

from __future__ import annotations

import dataclasses
import functools
import inspect
import math
import random
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

import msgspec

from loom.core.cache._single_flight import SingleFlight
from loom.core.cache.abc.backend import CacheBackend
from loom.core.cache.abc.config import CacheConfig
from loom.core.cache.decorators import _CallPolicy, declares_cache_call
from loom.core.cache.keys import call_key
from loom.core.cache.result_codec import ResultCodec, build_call_codec
from loom.core.di.container import LoomContainer
from loom.core.logger import get_logger
from loom.core.model.introspection import resolve_type_hints

AsyncCallable = Callable[..., Awaitable[Any]]

_UNDECODABLE = object()
"""Sentinel for a payload the codec refused; ``None`` is a legitimate value."""


class _Unrenderable(Exception):
    """An argument has no canonical rendering, so the call gets no key."""


class CachedCalls(Protocol):
    """Binder that turns declared coroutines into cached ones."""

    def wrap(self, func: AsyncCallable) -> AsyncCallable:
        """Return the cached form of *func*, or *func* itself."""
        ...

    def bind(self, obj: object) -> list[AsyncCallable]:
        """Return the public coroutine methods of *obj*, cached where declared."""
        ...


def cached_calls(container: LoomContainer) -> CachedCalls:
    """Return the container's binder, or a pass-through when it has none.

    A container built outside the two bootstraps that apply the cache module
    has no binding, and :meth:`LoomContainer.resolve` raises for one. A factory
    called with such a container gets the announcing pass-through instead, so
    it never has to guess how its application was built.

    Args:
        container: Container handed to the factory.

    Returns:
        The registered binder, or a fresh pass-through.
    """
    calls: CachedCalls | None = container.resolve_or_none(CachedCalls)
    return _UnconfiguredCalls() if calls is None else calls


def _discriminated(value: Any, rendered: Any) -> list[Any]:
    """Label *rendered* with the qualified type name of *value*.

    Every value the walk visits is labelled, native or not: deciding "native"
    by ``isinstance`` leaves an ``IntEnum``, a ``StrEnum`` and a ``str``
    subclass indistinguishable from the scalar they subclass, and deciding it
    by exact type is one more rule to keep right. A uniform label removes the
    question; the cost is a longer buffer behind a SHA-256, which nothing reads
    back.

    Args:
        value: The object being rendered, whose type is the label.
        rendered: Its rendering, already canonical.

    Returns:
        The two-element list ``[type_name, rendered]``.
    """
    cls = type(value)
    return [f"{cls.__module__}.{cls.__qualname__}", rendered]


def _render(value: Any) -> list[Any]:
    """Render *value* canonically, so equal arguments give one key.

    The walk descends structurally and hands :func:`msgspec.to_builtins` only a
    true leaf, so a ``datetime``, an ``Enum`` or a ``set`` nested inside a
    struct is reached before anything can flatten it. Every value the walk
    visits carries its qualified type name, which is what separates a
    ``datetime`` from its ISO string and a ``list`` from a ``tuple``.

    Args:
        value: One bound argument, or a part of one.

    Returns:
        The canonical rendering of *value*, discriminated by its type.

    Raises:
        _Unrenderable: The value, or something nested inside it, has no
            canonical rendering.
    """
    if isinstance(value, Mapping):
        return _discriminated(value, _render_mapping(value))
    if isinstance(value, set | frozenset):
        return _discriminated(value, _render_set(value))
    if isinstance(value, list | tuple):
        return _discriminated(value, [_render(item) for item in value])
    if isinstance(value, msgspec.Struct):
        return _discriminated(value, _render_mapping(msgspec.structs.asdict(value)))
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _discriminated(value, _render_dataclass(value))
    return _render_leaf(value)


def _render_dataclass(value: Any) -> list[list[Any]]:
    """Render a dataclass instance's fields, shallowly.

    A field declared ``init=False`` and never assigned has no value to read, so
    :func:`getattr` raises for it. That is an argument shape, not a failure of
    the body, so it costs the key and never the call.

    Args:
        value: The dataclass instance being rendered.

    Returns:
        Its fields, rendered as a mapping of name to value.

    Raises:
        _Unrenderable: A declared field holds no value.
    """
    try:
        fields = {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    except AttributeError as error:
        raise _Unrenderable(f"unreadable dataclass field: {error}") from error
    return _render_mapping(fields)


def _reject_non_finite(value: Any) -> None:
    """Refuse a float no digest can tell from the other two.

    ``nan``, ``inf`` and ``-inf`` all encode as ``null``, so they would share
    one entry with each other. The check runs on what the walk is handed *and*
    on what :func:`msgspec.to_builtins` gives back, because a leaf can render
    to a raw float the walk never saw: an ``Enum`` valued ``nan`` is one.

    Args:
        value: A value about to be rendered, or a rendering.

    Raises:
        _Unrenderable: The value is a non-finite float.
    """
    if isinstance(value, float) and not math.isfinite(value):
        raise _Unrenderable(f"non-finite float: {value!r}")


def _render_mapping(value: Mapping[Any, Any]) -> list[list[Any]]:
    """Render a mapping as pairs sorted by their whole encoding.

    A list of pairs, not a dict: two keys whose renderings coincide must stay
    two entries. Building a dict here dropped one of them and made the digest
    describe an argument that was never passed. Sorting by the encoded *pair*
    rather than the encoded key means ties are byte-identical pairs, so
    :meth:`list.sort`'s stability can no longer leak insertion order.

    Args:
        value: The mapping being rendered.

    Returns:
        Its key/value pairs, each rendered, in a total order.
    """
    pairs = [[_render(key), _render(item)] for key, item in value.items()]
    pairs.sort(key=_encoded)
    return pairs


def _render_set(value: set[Any] | frozenset[Any]) -> list[Any]:
    """Render a set as a list ordered by its members' encodings."""
    return sorted((_render(item) for item in value), key=_encoded)


def _render_leaf(value: Any) -> list[Any]:
    """Render one value the walk found no structure in.

    An object :func:`msgspec.to_builtins` expands into a container — an
    ``attrs`` class, say — is walked once more, on msgspec's own output. What
    that expansion erased stays erased, exactly as it is today.

    The rendering is checked for a non-finite float as well as the value: what
    arrives here may be a scalar the walk never saw, and an ``Enum`` valued
    ``nan`` renders to one.

    Args:
        value: A value the walk found no structure in.

    Returns:
        Its rendering, discriminated by its type.

    Raises:
        _Unrenderable: msgspec has no rendering for it, or renders it to a
            non-finite float.
    """
    try:
        rendered = msgspec.to_builtins(value)
    except (TypeError, ValueError) as error:
        raise _Unrenderable(f"no canonical rendering: {error}") from error
    _reject_non_finite(rendered)
    if isinstance(rendered, Mapping | list | tuple | set | frozenset):
        return _discriminated(value, _render(rendered))
    return _discriminated(value, rendered)


def _encoded(value: Any) -> bytes:
    """Encode a rendered value, giving both a sort order and a check.

    Raises:
        _Unrenderable: The rendering cannot be encoded, so it can neither be
            ordered nor hashed.
    """
    try:
        return msgspec.json.encode(value)
    except (TypeError, ValueError, msgspec.MsgspecError) as error:
        raise _Unrenderable(f"cannot encode {type(value).__name__}: {error}") from error


@dataclass(frozen=True, slots=True)
class _CallSpec:
    """Everything about one wrapped call that is decided at wrap time.

    Attributes:
        func: The declared coroutine, unwrapped.
        policy: What ``@cache_call`` declared about it.
        codec: Codec derived from its return annotation.
        signature: Its signature, computed once.
    """

    func: AsyncCallable
    policy: _CallPolicy
    codec: ResultCodec
    signature: inspect.Signature

    def key(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        """Build the cache key of one call.

        Args:
            args: Positional arguments of the call.
            kwargs: Keyword arguments of the call.

        Returns:
            The key covering this function, this version and these arguments.

        Raises:
            _Unrenderable: An argument has no canonical rendering, or its
                rendering is not encodable — no argument shape may raise to the
                caller, so the walk and the final encoding reach the same
                guard, an argument that refers to itself included.
            TypeError: The arguments do not fit the signature; the body would
                raise the same error.
        """
        bound = self.signature.bind(*args, **kwargs)
        bound.apply_defaults()
        try:
            arguments = _render(dict(bound.arguments))
        except RecursionError as error:
            raise _Unrenderable(f"refers to itself, or nests too deeply: {error}") from error
        try:
            return call_key(
                module=self.func.__module__,
                qualname=self.func.__qualname__,
                version=self.policy.version,
                arguments=arguments,
            )
        except (RecursionError, TypeError, ValueError, msgspec.MsgspecError) as error:
            raise _Unrenderable(f"cannot encode the rendered arguments: {error}") from error


def _function_name(func: AsyncCallable) -> str:
    """Dotted ``module.qualname`` of *func*."""
    return f"{func.__module__}.{func.__qualname__}"


def _publishes_coroutine(attribute: object) -> bool:
    """Whether a class-dictionary entry publishes a coroutine.

    The raw entry is asked rather than the instance attribute, so a property
    is never evaluated. A ``staticmethod`` or ``classmethod`` entry is the
    descriptor and not the function it holds, so it is unwrapped first:
    asking the descriptor itself answers ``False`` for an async static or
    class method and would drop the tool without a word.

    Args:
        attribute: Entry read from a class ``__dict__``.

    Returns:
        Whether binding the entry to an instance yields a coroutine function.
    """
    if isinstance(attribute, staticmethod | classmethod):
        return inspect.iscoroutinefunction(attribute.__func__)
    return inspect.iscoroutinefunction(attribute)


def _log_abandoned_call(key: str, error: BaseException) -> None:
    """Report a detached load that failed with no caller left to receive it.

    The body of a cached call is arbitrary application code, so a failure the
    single flight can hand to nobody would otherwise disappear entirely.
    """
    get_logger(__name__).warning("CacheCallLoadAbandoned", key=key, error=repr(error))


def _coroutine_methods(obj: object, warn_once: _WarnOnce) -> list[AsyncCallable]:
    """Return the public coroutine methods of *obj*, base classes first.

    The MRO is walked in reverse and each class dictionary in its own order, so
    a method keeps the position of the class that first declared it and an
    override does not jump to the end. The **class attribute** is what decides,
    not the instance one: reading the instance would evaluate a property, and a
    property is not a tool. A ``staticmethod`` or ``classmethod`` is unwrapped
    before the question is asked, so an async one is published like any other
    coroutine method.

    A subclass that shadows a base's coroutine with something that is not one —
    a property, a plain ``def`` — unpublishes it, because the published object
    would not be awaitable. That is announced (``CacheCallOverriddenBySync``):
    a tool disappearing from a toolset is exactly the kind of change nobody
    notices until an agent stops using it.

    Args:
        obj: Instance whose methods are being published.
        warn_once: Announcer of the binder asking for the methods.

    Returns:
        The bound methods, in declaration order.
    """
    names: dict[str, None] = {}
    for cls in reversed(type(obj).__mro__):
        for name, attribute in cls.__dict__.items():
            if name.startswith("_"):
                continue
            if _publishes_coroutine(attribute):
                names[name] = None
            elif name in names:
                del names[name]
                warn_once.emit(
                    "CacheCallOverriddenBySync",
                    f"{cls.__qualname__}.{name}",
                    attribute=type(attribute).__name__,
                )
    return [cast(AsyncCallable, getattr(obj, name)) for name in names]


class _WarnOnce:
    """Per-instance memory of which warning was already emitted for which call.

    The scope is the binder the composition root built, not the module: two
    containers in one process warn independently, and no import mutates
    anything global.
    """

    __slots__ = ("_log", "_seen")

    def __init__(self) -> None:
        self._log = get_logger(__name__)
        self._seen: set[tuple[str, str]] = set()

    def __call__(self, event: str, func: AsyncCallable, **fields: Any) -> None:
        """Log *event* for *func* the first time it happens.

        Args:
            event: Log event name.
            func: Declared coroutine the event is about.
            **fields: Extra structured fields.
        """
        self.emit(event, _function_name(func), **fields)

    def emit(self, event: str, name: str, **fields: Any) -> None:
        """Log *event* for the dotted *name* the first time it happens.

        Args:
            event: Log event name.
            name: Dotted name of what the event is about; the memo is keyed by
                the pair, so two events about one name both get through and one
                event about two names does too.
            **fields: Extra structured fields.
        """
        if (event, name) in self._seen:
            return
        self._seen.add((event, name))
        self._log.warning(event, function=name, **fields)


class _ConfiguredCalls:
    """Binder over a live cache backend.

    Args:
        config: The decoded ``cache:`` section, for the TTLs.
        cache: Backend the entries are written to and read from.
        flight: Coalescer shared by every call this binder wraps.
        rng: Generator owned by this binder, so seeding the process-wide
            ``random`` module does not freeze every TTL in the process.
    """

    def __init__(
        self,
        config: CacheConfig,
        cache: CacheBackend,
        flight: SingleFlight,
        rng: random.Random,
    ) -> None:
        self._config = config
        self._cache = cache
        self._flight = flight
        self._rng = rng
        self._warn_once = _WarnOnce()

    def wrap(self, func: AsyncCallable) -> AsyncCallable:
        """Return the cached form of *func*.

        *func* comes back unchanged when it is already a wrapper, when it
        carries no policy, and when its return annotation yields no codec — the
        last case warns, because a deployment should learn that a marked
        coroutine runs uncached.

        Args:
            func: Coroutine, marked or not.

        Returns:
            The wrapper, or *func* itself.
        """
        if getattr(func, "__cache_call_wrapped__", False):
            return func
        policy = declares_cache_call(func)
        if policy is None:
            return func
        codec = build_call_codec(func)
        if codec is None:
            self._warn_once("CacheCallNotCacheable", func)
            return func
        spec = _CallSpec(func, policy, codec, inspect.signature(func))

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self._call(spec, args, kwargs)

        wrapper.__cache_call_wrapped__ = True  # type: ignore[attr-defined]
        wrapper.__annotations__ = resolve_type_hints(func, include_extras=True)
        return wrapper

    def bind(self, obj: object) -> list[AsyncCallable]:
        """Return the public coroutine methods of *obj*, cached where declared.

        Every public coroutine method is published, so adding a public helper
        to a toolset class publishes a tool.

        Args:
            obj: Instance to publish.

        Returns:
            The bound methods, in declaration order, base class first.
        """
        return [self.wrap(method) for method in _coroutine_methods(obj, self._warn_once)]

    async def _call(self, spec: _CallSpec, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        """Serve one call from the cache, or load it once and store it."""
        try:
            key = spec.key(args, kwargs)
        except _Unrenderable as error:
            self._warn_once("CacheCallKeyUnrenderable", spec.func, error=str(error))
            return await spec.func(*args, **kwargs)

        payload = await self._read(spec, key)
        if payload is not None:
            decoded = self._decode(spec, key, payload)
            if decoded is not _UNDECODABLE:
                return decoded
        return await self._flight.run(key, functools.partial(self._load, spec, key, args, kwargs))

    async def _read(self, spec: _CallSpec, key: str) -> Any:
        """Read the stored payload, answering a miss when the backend is down.

        A cache is an optimisation, and a Redis outage that raised here would
        fail every cached call in the process — trading a cache problem for an
        application outage, which is exactly what the write path already
        refuses to do. The transport raises whatever its client defines, so
        the guard is deliberately wide; cancellation is a ``BaseException``
        and still propagates.

        Returns:
            The stored payload, or ``None`` — the backends' miss sentinel.
        """
        try:
            return await self._cache.get_value(key)
        except Exception as error:
            self._warn_once("CacheCallReadFailed", spec.func, key=key, error=type(error).__name__)
            return None

    def _decode(self, spec: _CallSpec, key: str, payload: Any) -> Any:
        """Decode a cached payload, or report it unusable.

        A payload written before the declared type gained a field no longer
        fits it, and nothing about the entry changes when the type does.
        The entry is treated as a miss and the load that follows overwrites
        it.

        Returns:
            The decoded value, or :data:`_UNDECODABLE`, which is not ``None``
            because ``None`` is a value a codec can legitimately return.
        """
        try:
            return spec.codec.decode(payload)
        except (msgspec.ValidationError, ValueError, TypeError) as error:
            self._warn_once(
                "CacheCallPayloadMismatch", spec.func, key=key, error=type(error).__name__
            )
            return _UNDECODABLE

    async def _load(
        self,
        spec: _CallSpec,
        key: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        """Run the body once and store its result, unless the policy refuses.

        The encoded value is returned rather than the body's own object, so a
        hit and a miss answer the same shape — except when ``unless`` is
        truthy, the one case where the caller sees what the body built.
        """
        result = await spec.func(*args, **kwargs)
        if spec.policy.unless is not None and spec.policy.unless(result):
            return result
        encoded = spec.codec.encode(result)
        try:
            await self._cache.set_value(key, encoded.payload, ttl=self._write_ttl(spec.policy))
        except Exception as error:
            self._warn_once(
                "CacheCallWriteFailed",
                spec.func,
                key=key,
                value_type=type(result).__name__,
                error=repr(error),
            )
        return encoded.value

    def _write_ttl(self, policy: _CallPolicy) -> int:
        """Resolve the declared TTL and spread it inside the configured jitter.

        ``ttl_key`` shares the ``ttl:`` namespace with entity overrides, so a
        key equal to an entity name deliberately shares that entity's TTL. The
        spread is applied where the value reaches the backend, so a burst of
        writes does not expire at the same instant; with ``ttl_jitter`` at zero
        the resolved value is written as it is.
        """
        ttl = (
            self._config.default_ttl
            if policy.ttl_key is None
            else self._config.ttl_for_single(policy.ttl_key)
        )
        jitter = self._config.ttl_jitter
        if jitter <= 0.0:
            return ttl
        spread = ttl * jitter
        return max(1, round(ttl + self._rng.uniform(-spread, spread)))


class _UnconfiguredCalls:
    """Pass-through binder for a deployment with no usable ``cache:`` section.

    It announces, once per marked coroutine it is asked to bind, that the
    coroutine runs uncached, so the deployment learns at boot rather than from
    its bill.
    """

    __slots__ = ("_warn_once",)

    def __init__(self) -> None:
        self._warn_once = _WarnOnce()

    def wrap(self, func: AsyncCallable) -> AsyncCallable:
        """Return *func* unchanged, announcing it when it is marked.

        Args:
            func: Coroutine, marked or not.

        Returns:
            *func* itself.
        """
        if declares_cache_call(func) is not None:
            self._warn_once("CacheCallNotConfigured", func)
        return func

    def bind(self, obj: object) -> list[AsyncCallable]:
        """Return the public coroutine methods of *obj*, all uncached.

        Args:
            obj: Instance to publish.

        Returns:
            The bound methods, in declaration order, base class first.
        """
        return [self.wrap(method) for method in _coroutine_methods(obj, self._warn_once)]
