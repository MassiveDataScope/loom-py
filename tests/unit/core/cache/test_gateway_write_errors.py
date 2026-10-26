"""A cache write that cannot be serialised fails loud and stores nothing (AC6)."""

from __future__ import annotations

from typing import Any

import pytest
from aiocache import caches

from loom.core.cache import CacheConfig, CacheGateway, CacheWriteError
from loom.core.model.redaction import RedactedMap

from ._doubles import MEMORY_BACKEND, SERIALIZED_BACKEND


def _gateway(alias: str, backend: dict[str, Any]) -> CacheGateway:
    CacheGateway.apply_config(
        CacheConfig(aiocache_alias=alias, aiocache_config=RedactedMap({alias: backend}))
    )
    return CacheGateway(alias=alias)


class _RecordingBackend:
    """Stand-in for the aiocache ``set`` method that records its call."""

    def __init__(self, failure: Exception | None = None) -> None:
        self.failure = failure
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def __call__(self, *args: Any, **kwargs: Any) -> bool:
        self.calls.append((args, kwargs))
        if self.failure is not None:
            raise self.failure
        return True


class TestSerializedBackend:
    async def test_a_value_the_serializer_rejects_raises_and_stores_nothing(self) -> None:
        gateway = _gateway("serialised", SERIALIZED_BACKEND)

        with pytest.raises(CacheWriteError, match=r"'k'.*object"):
            await gateway.set_value("k", object())

        assert await gateway.get_value("k") is None

    async def test_one_rejected_pair_stores_none_of_the_batch(self) -> None:
        gateway = _gateway("serialised", SERIALIZED_BACKEND)

        with pytest.raises(CacheWriteError, match=r"'bad'"):
            await gateway.multi_set_values([("a", 1), ("b", 2), ("bad", object())])

        assert await gateway.multi_get_values(["a", "b", "bad"]) == [None, None, None]

    async def test_a_backend_failure_propagates_unwrapped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gateway = _gateway("serialised", SERIALIZED_BACKEND)
        backend = _RecordingBackend(ValueError("backend down"))
        monkeypatch.setattr(caches.get("serialised"), "set", backend)

        with pytest.raises(ValueError, match="backend down") as info:
            await gateway.set_value("k", {"fine": 1})

        assert not isinstance(info.value, CacheWriteError)


class TestRawBackend:
    async def test_a_raw_alias_stores_the_object_itself(self) -> None:
        gateway = _gateway("raw", MEMORY_BACKEND)
        value = object()

        await gateway.set_value("k", value)

        assert await gateway.get_value("k") is value

    async def test_a_raw_alias_calls_the_backend_without_a_dumps_fn(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gateway = _gateway("raw", MEMORY_BACKEND)
        backend = _RecordingBackend()
        monkeypatch.setattr(caches.get("raw"), "set", backend)
        value = object()

        await gateway.set_value("k", value, ttl=7)

        assert backend.calls == [(("k", value), {"ttl": 7})]
