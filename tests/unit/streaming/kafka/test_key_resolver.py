from __future__ import annotations

import pytest

from loom.streaming.kafka import FixedKey, KafkaRecord, PartitionKeyResolver, PreserveKey

pytestmark = pytest.mark.kafka


class TestPreserveKey:
    def test_preserve_key_returns_encoded_string_key(self) -> None:
        resolver = PreserveKey[int]()
        record = KafkaRecord(topic="orders", key="tenant-a", value=1)

        assert resolver.resolve(record) == b"tenant-a"

    def test_preserve_key_returns_none_when_missing(self) -> None:
        resolver = PreserveKey[int]()
        record = KafkaRecord(topic="orders", key=None, value=1)

        assert resolver.resolve(record) is None


class TestFixedKey:
    def test_fixed_key_always_returns_same_value(self) -> None:
        resolver: PartitionKeyResolver[int] = FixedKey(value=b"fixed")
        record = KafkaRecord(topic="orders", key=None, value=1)

        assert resolver.resolve(record) == b"fixed"
