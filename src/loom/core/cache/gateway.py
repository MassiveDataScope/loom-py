from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

import msgspec
from aiocache import caches  # type: ignore[import-untyped]

from loom.core.cache.errors import CacheWriteError
from loom.core.cache.serializer import MsgspecSerializer
from loom.core.config.errors import ConfigError
from loom.core.model import loom_type_of

if TYPE_CHECKING:
    from loom.core.cache.abc.config import CacheConfig

T = TypeVar("T")

_SIMPLE_MEMORY_CACHE = "SimpleMemoryCache"
_BOUNDED_MEMORY_CACHE = "loom.core.cache.memory.BoundedMemoryCache"
_DEFAULT_ALIAS = "default"
_MEMORY_FALLBACK = {"cache": "aiocache.SimpleMemoryCache"}


def _identity(value: Any) -> Any:
    """``dumps_fn`` handed to aiocache once loom has already encoded the value."""
    return value


def _is_raw_backend(cache: Any) -> bool:
    """Return ``True`` when the aiocache backend stores values without serialization.

    A backend is considered "raw" when its configured serializer is NOT
    :class:`~loom.core.cache.serializer.MsgspecSerializer`.  Raw backends
    support native atomic increment operations (Redis ``INCR``,
    ``SimpleMemoryCache.increment``) because there are no encoded bytes to
    corrupt.
    """
    return not isinstance(getattr(cache, "serializer", None), MsgspecSerializer)


def _require_declared_aliases(config: CacheConfig) -> None:
    """Refuse a data or counter alias that has no entry in ``aiocache_config``.

    The ``default`` alias is exempt: :meth:`CacheGateway.apply_config` supplies
    a fallback for it.
    """
    for declared in (config.aiocache_alias, config.effective_counter_alias):
        if declared != _DEFAULT_ALIAS and declared not in config.aiocache_config:
            raise ConfigError(
                f"Cache alias {declared!r} is declared but missing from 'aiocache_config'."
            )


def _translate_backend(backend_cfg: Any, config: CacheConfig) -> Any:
    """Return the aiocache entry for one alias.

    Non-dict entries are forwarded as they are.  Dict entries are copied, and a
    bounded ``SimpleMemoryCache`` entry becomes a ``BoundedMemoryCache`` one.
    """
    if not isinstance(backend_cfg, dict):
        return backend_cfg
    entry: dict[str, Any] = backend_cfg
    if _is_bounded(entry, config) and _SIMPLE_MEMORY_CACHE in str(entry.get("cache", "")):
        return _bounded_memory_entry(entry, config)
    return dict(entry)


def _is_bounded(entry: Mapping[str, Any], config: CacheConfig) -> bool:
    """Return ``True`` when the config or the entry itself sets a memory bound."""
    return (
        config.max_size is not None
        or config.max_bytes is not None
        or "max_size" in entry
        or "max_bytes" in entry
    )


def _bounded_memory_entry(entry: Mapping[str, Any], config: CacheConfig) -> dict[str, Any]:
    """Copy *entry* onto ``BoundedMemoryCache``; the entry's own bounds win."""
    bounded = {**entry, "cache": _BOUNDED_MEMORY_CACHE}
    for key, bound in (("max_size", config.max_size), ("max_bytes", config.max_bytes)):
        if bound is not None:
            bounded.setdefault(key, bound)
    return bounded


class CacheGateway:
    """Facade over aiocache with msgpack serialization for entity data.

    Two distinct usage modes:

    **Data gateway** — configured with
    :class:`~loom.core.cache.serializer.MsgspecSerializer`.  Used by
    :class:`~loom.core.cache.repository.CachedRepository` for entity and list
    storage.  All values are msgpack-encoded on write and decoded on read.

    **Counter gateway** — configured *without* a serializer (raw backend).
    Used by :class:`~loom.core.cache.dependency.GenerationalDependencyResolver`
    for generation counter storage.  Values are stored as plain Python integers,
    enabling atomic native increment on both Redis and ``SimpleMemoryCache``.

    The gateway auto-detects which mode it is in at construction time and routes
    :meth:`incr` accordingly — no manual configuration needed beyond choosing the
    right aiocache alias.

    Args:
        alias: Registered aiocache alias to retrieve the backend from.

    Example::

        data_gateway    = CacheGateway(alias=config.aiocache_alias)
        counter_gateway = CacheGateway(alias=config.effective_counter_alias)
        resolver = GenerationalDependencyResolver(counter_gateway)
    """

    def __init__(self, *, alias: str = "default") -> None:
        """Initialise the gateway using the named aiocache alias.

        Args:
            alias: Registered aiocache alias to retrieve the backend from.
        """
        # aiocache ships no stubs: its backend is ``Any`` to both type checkers.
        self._cache: Any = caches.get(alias)
        # Raw backends store values as they are: no encoding on write, and
        # native atomic increment.
        self._raw_backend: bool = _is_raw_backend(self._cache)

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------

    @staticmethod
    def configure(raw_config: Mapping[str, Any]) -> None:
        """Apply a configuration mapping to the global aiocache registry.

        Args:
            raw_config: Configuration dict compatible with ``aiocache.caches.set_config``.
        """
        caches.set_config(dict(raw_config))

    @classmethod
    def apply_config(cls, config: CacheConfig) -> None:
        """Configure aiocache from a :class:`~loom.core.cache.abc.config.CacheConfig`.

        Equivalent to :meth:`configure`, except that an ``aiocache.SimpleMemoryCache``
        entry with a bound — ``config.max_size``, ``config.max_bytes``, or its own
        ``max_size``/``max_bytes``, which win — becomes a
        :class:`~loom.core.cache.memory.BoundedMemoryCache`.  Entries for other
        backend types (Redis, Memcached, …) are forwarded unchanged.

        Args:
            config: Resolved cache configuration.

        Example::

            cache_cfg = CacheConfig(
                aiocache_alias="cache",
                counter_alias="counters",
                max_size=1000,
                aiocache_config={
                    "cache":    {"cache": "aiocache.SimpleMemoryCache", ...},
                    "counters": {"cache": "aiocache.SimpleMemoryCache"},
                },
            )
            CacheGateway.apply_config(cache_cfg)
        """
        _require_declared_aliases(config)
        raw: dict[str, Any] = {
            alias: _translate_backend(backend_cfg, config)
            for alias, backend_cfg in config.aiocache_config.items()
        }
        if _DEFAULT_ALIAS not in raw:
            raw[_DEFAULT_ALIAS] = dict(raw.get(config.aiocache_alias) or _MEMORY_FALLBACK)
        cls.configure(raw)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    @overload
    async def get_value(self, key: str, *, type: type[T]) -> T | None: ...

    @overload
    async def get_value(self, key: str, *, type: None = ...) -> Any: ...

    # Kept for structural typing: without it ``CacheGateway`` no longer
    # satisfies the ``CacheBackend`` protocol ``CachedRepository`` asks for.
    @overload
    async def get_value(self, key: str, *, type: type[T] | None = None) -> T | Any | None: ...

    async def get_value(self, key: str, *, type: type[T] | None = None) -> T | Any | None:
        """Retrieve a cached value, optionally converting it to the given type.

        Args:
            key: Cache key.
            type: Optional target type, converted through the type's
                :class:`~loom.core.model.LoomType`.

        Returns:
            The cached value (converted to ``type`` if given) or ``None`` on miss.

        Raises:
            BoundaryValidationError: The cached value does not match ``type``.
        """
        value = await self._cache.get(key)
        if value is None:
            return None
        if type is None:
            return value
        return loom_type_of(type).from_builtins(value)

    async def multi_get_values(
        self,
        keys: list[str],
        *,
        type: type[T] | None = None,
    ) -> list[T | Any | None]:
        """Retrieve multiple values in a single round-trip.

        Args:
            keys: Cache keys to look up.
            type: Optional target type, converted through the type's
                :class:`~loom.core.model.LoomType` on each value.

        Returns:
            Values in the same order as ``keys``, with ``None`` for misses.

        Raises:
            BoundaryValidationError: A cached value does not match ``type``.
        """
        values = await self._cache.multi_get(keys)
        if type is not None:
            compiled = loom_type_of(type)
            return [
                compiled.from_builtins(value) if value is not None else None for value in values
            ]
        return cast(list[T | Any | None], values)

    async def exists(self, key: str) -> bool:
        """Check whether a key exists in the cache.

        Args:
            key: Cache key to check.

        Returns:
            ``True`` if the key is present.
        """
        return bool(await self._cache.exists(key))

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def _encode(self, key: str, value: Any) -> Any:
        """Serialise *value* with the backend serializer, naming the key on failure."""
        try:
            return self._cache.serializer.dumps(value)
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            raise CacheWriteError(
                f"cache key {key!r}: cannot serialise {type(value).__name__}"
            ) from exc

    async def set_value(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Store a value under the given key.

        On a serialised backend the value is encoded here, so a value the
        serializer rejects raises :class:`~loom.core.cache.errors.CacheWriteError`
        and nothing reaches the backend.  A raw backend stores *value* as is.

        Args:
            key: Cache key.
            value: Value to store.
            ttl: Time-to-live in seconds. ``None`` means no expiration.

        Raises:
            CacheWriteError: If the backend serializer cannot encode *value*.
        """
        if self._raw_backend:
            await self._cache.set(key, value, ttl=ttl)
            return
        await self._cache.set(key, self._encode(key, value), ttl=ttl, dumps_fn=_identity)

    async def multi_set_values(
        self,
        pairs: list[tuple[str, Any]],
        ttl: int | None = None,
    ) -> None:
        """Store multiple key-value pairs in a single round-trip.

        On a serialised backend every pair is encoded before any is written,
        so one rejected value stores none of them.

        Args:
            pairs: List of ``(key, value)`` tuples.
            ttl: Time-to-live in seconds applied to all entries.

        Raises:
            CacheWriteError: If the backend serializer cannot encode a value.
        """
        if self._raw_backend:
            await self._cache.multi_set(pairs, ttl=ttl)
            return
        encoded = [(key, self._encode(key, value)) for key, value in pairs]
        await self._cache.multi_set(encoded, ttl=ttl, dumps_fn=_identity)

    async def incr(self, key: str, delta: int = 1) -> int:
        """Increment a numeric value at the given key.

        Routes to the optimal strategy based on the backend configuration
        detected at construction time:

        - **Raw backend** (no serializer): delegates to the backend's native
          ``increment`` method when available.  On Redis this is an atomic
          ``INCR`` / ``INCRBY`` command; on ``SimpleMemoryCache`` it is
          asyncio-safe.  Falls back to GET+SET for backends that do not expose
          ``increment``.
        - **Serialized backend** (``MsgspecSerializer``): uses a non-atomic
          GET+SET.  Correct for single-process deployments; the asyncio event
          loop does not preempt between the two awaits within a single
          coroutine execution.

        Args:
            key: Cache key holding an integer value.
            delta: Amount to increment by. Defaults to ``1``.

        Returns:
            The new value after incrementing.
        """
        if self._raw_backend:
            return await self._native_incr(key, delta)
        return await self._serialized_incr(key, delta)

    async def _native_incr(self, key: str, delta: int) -> int:
        raw = getattr(self._cache, "increment", None)
        if callable(raw):
            fn = cast(Callable[[str, int], Awaitable[Any]], raw)
            return int(await fn(key, delta))
        return await self._serialized_incr(key, delta)

    async def _serialized_incr(self, key: str, delta: int) -> int:
        current = await self.get_value(key, type=int)
        next_value = (0 if current is None else int(current)) + delta
        await self.set_value(key, next_value)
        return next_value

    # ------------------------------------------------------------------
    # Delete / maintenance
    # ------------------------------------------------------------------

    async def delete(self, key: str) -> int:
        """Delete a single key from the cache.

        Args:
            key: Cache key to remove.

        Returns:
            Number of keys actually deleted (0 or 1).
        """
        return int(await self._cache.delete(key))

    async def delete_many(self, keys: list[str]) -> int:
        """Delete multiple keys from the cache.

        Args:
            keys: Cache keys to remove.

        Returns:
            Number of keys actually deleted.
        """
        return int(await self._cache.multi_delete(keys))

    async def clear(self) -> None:
        """Remove all entries from the cache backend."""
        await self._cache.clear()

    async def close(self) -> None:
        """Release the underlying cache connection resources."""
        await self._cache.close()
