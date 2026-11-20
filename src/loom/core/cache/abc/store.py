from __future__ import annotations

from typing import Any, Protocol, TypeVar

T = TypeVar("T")


class CacheBackend(Protocol):
    """Abstract cache backend defining the contract for key-value storage operations."""

    async def get_value(self, key: str, *, type: type[T] | None = None) -> T | Any | None:
        """Retrieve a value by key, optionally converting it to the given type.

        Args:
            key: Cache key to look up.
            type: Optional target type for deserialization.

        Returns:
            The cached value, converted to ``type`` when provided, or ``None`` on miss.
        """
        ...

    async def set_value(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Store a value under the given key with an optional TTL in seconds.

        Args:
            key: Cache key.
            value: Value to store.
            ttl: Time-to-live in seconds. ``None`` means no expiration.
        """
        ...

    async def multi_get_values(
        self,
        keys: list[str],
        *,
        type: type[T] | None = None,
    ) -> list[T | Any | None]:
        """Retrieve multiple values by their keys in a single round-trip.

        Args:
            keys: List of cache keys to look up.
            type: Optional target type for deserialization of each value.

        Returns:
            A list of values in the same order as ``keys``, with ``None`` for misses.
        """
        ...

    async def multi_set_values(
        self,
        pairs: list[tuple[str, Any]],
        ttl: int | None = None,
    ) -> None:
        """Store multiple key-value pairs in a single round-trip.

        Args:
            pairs: List of ``(key, value)`` tuples to store.
            ttl: Time-to-live in seconds applied to all entries.
        """
        ...

    async def exists(self, key: str) -> bool:
        """Check whether a key exists in the cache.

        Args:
            key: Cache key to check.

        Returns:
            ``True`` if the key is present, ``False`` otherwise.
        """
        ...

    async def delete(self, key: str) -> int:
        """Delete a single key from the cache.

        Args:
            key: Cache key to remove.

        Returns:
            Number of keys actually deleted (0 or 1).
        """
        ...

    async def delete_many(self, keys: list[str]) -> int:
        """Delete multiple keys from the cache in a single operation.

        Args:
            keys: List of cache keys to remove.

        Returns:
            Number of keys actually deleted.
        """
        ...

    async def incr(self, key: str, delta: int = 1) -> int:
        """Atomically increment a numeric value stored at the given key.

        Args:
            key: Cache key holding an integer value.
            delta: Amount to increment by. Defaults to ``1``.

        Returns:
            The new value after incrementing.
        """
        ...

    async def close(self) -> None:
        """Release any resources held by the backend (connections, pools, etc.)."""
        ...
