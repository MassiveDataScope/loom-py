"""An in-memory aiocache backend that evicts when it gets too big.

``aiocache.SimpleMemoryCache`` accepts no bound: passing it ``max_size`` fails at
construction, and without one it only shrinks by TTL. This backend is that class
plus a ledger, with the same contract, a limit on entries and/or bytes, and
least-recently-used eviction the moment a write crosses either one.
"""

from __future__ import annotations

import asyncio
import sys
from collections import OrderedDict
from typing import Any

from aiocache import SimpleMemoryCache  # type: ignore[import-untyped]

__all__ = ["BoundedMemoryCache"]


def _cost(value: object) -> int:
    """Bytes *value* is charged for.

    ``bytes`` and ``str`` get their exact wire length because that is what a
    serialized alias stores; anything else is a raw object on an unserialized
    alias, where ``getsizeof``'s rough estimate is all there is.
    """
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value)
    if isinstance(value, str):
        return len(value.encode())
    return sys.getsizeof(value)


class _Ledger(OrderedDict[str, Any]):
    """The store: an ``OrderedDict`` in recency order that also counts bytes.

    ``SimpleMemoryBackend`` only ever calls ``get``, ``__setitem__``, ``pop``,
    ``__contains__`` and ``list()`` on its store, so those keep the order and the
    byte count right whichever path of the parent touches them. The cost is
    recorded at write time and refunded from that record: a caller may mutate a
    raw value it still references, so recomputing it on delete would drift.
    """

    def __init__(self) -> None:
        super().__init__()
        self.bytes = 0
        self._costs: dict[str, int] = {}

    def get(self, key: str, default: Any = None) -> Any:
        if key in self:
            self.move_to_end(key)
            return super().__getitem__(key)
        return default

    def __setitem__(self, key: str, value: Any) -> None:
        cost = _cost(value)
        self.bytes += cost - self._costs.get(key, 0)
        self._costs[key] = cost
        super().__setitem__(key, value)
        self.move_to_end(key)

    def __delitem__(self, key: str) -> None:
        super().__delitem__(key)
        self.bytes -= self._costs.pop(key)

    def pop(self, key: str, default: Any = None) -> Any:
        if key not in self:
            return default
        value = super().__getitem__(key)
        del self[key]
        return value

    def popitem(self, last: bool = True) -> tuple[str, Any]:
        key, value = super().popitem(last)
        self.bytes -= self._costs.pop(key)
        return key, value

    def clear(self) -> None:
        super().clear()
        self._costs.clear()
        self.bytes = 0


class BoundedMemoryCache(SimpleMemoryCache):  # type: ignore[misc]
    """``SimpleMemoryCache`` with a ceiling on entries and bytes, evicting LRU.

    Args:
        max_size: Most entries kept at once, or ``None`` for no such bound.
        max_bytes: Most bytes kept at once, measured on the stored payload, or
            ``None`` for no such bound.
        **kwargs: Everything ``SimpleMemoryCache`` accepts.

    A single value larger than ``max_bytes`` is stored and evicts everything
    else: refusing it would make ``get`` after ``set`` miss, which the contract
    does not allow. A ``set`` whose ``_cas_token`` does not match stores nothing
    but still counts as a use of the key.
    """

    NAME = "bounded_memory"

    def __init__(
        self,
        *,
        max_size: int | None = None,
        max_bytes: int | None = None,
        **kwargs: Any,
    ) -> None:
        for name, bound in (("max_size", max_size), ("max_bytes", max_bytes)):
            if bound is not None and bound < 1:
                raise ValueError(f"{name} must be at least 1, got {bound}")
        super().__init__(**kwargs)
        self.max_size = max_size
        self.max_bytes = max_bytes
        self.evictions = 0
        self._ledger = _Ledger()
        self._cache = self._ledger
        self._handlers: dict[str, asyncio.TimerHandle] = {}

    @property
    def bytes(self) -> int:
        """Bytes the stored payloads occupy right now."""
        return self._ledger.bytes

    async def _set(
        self,
        key: str,
        value: Any,
        ttl: Any = None,
        _cas_token: Any = None,
        _conn: Any = None,
    ) -> Any:
        stored = await super()._set(key, value, ttl=ttl, _cas_token=_cas_token, _conn=_conn)
        if stored:
            self._evict()
        return stored

    async def _increment(self, key: str, delta: int, _conn: Any = None) -> Any:
        result = await super()._increment(key, delta, _conn=_conn)
        self._evict()
        return result

    async def _clear(self, namespace: Any = None, _conn: Any = None) -> Any:
        if namespace:
            return await super()._clear(namespace, _conn=_conn)
        self._ledger.clear()
        for handle in self._handlers.values():
            handle.cancel()
        self._handlers.clear()
        return True

    def _evict(self) -> None:
        """Drop least recently used entries until both bounds hold.

        The entry just written is the most recent, so the ``> 1`` guard is what
        keeps it when it alone is over the byte bound.
        """
        while len(self._ledger) > 1 and self._over():
            key, _ = self._ledger.popitem(last=False)
            handle = self._handlers.pop(key, None)
            if handle:
                handle.cancel()
            self.evictions += 1

    def _over(self) -> bool:
        if self.max_size is not None and len(self._ledger) > self.max_size:
            return True
        return self.max_bytes is not None and self._ledger.bytes > self.max_bytes
