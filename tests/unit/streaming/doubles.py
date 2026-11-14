"""Test doubles shared by more than one streaming package."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming import Message


class Order(LoomStruct):
    """Mutable order shared by compiler and contract tests.

    The name stays ``Order`` because route labels sort ``type.__name__`` and a
    lifecycle test asserts ``loom.declared_routes == "Order,Result"``. The frozen
    Bytewax order is ``AcmeOrder``.
    """

    order_id: str


class AcmePayload(LoomStruct):
    """Payload shared by handler, error-boundary, logging, and With tests."""

    value: str


class AcmeOrderEvent(LoomFrozenStruct, frozen=True):
    """Order event shared by multi-source compiler, contract, and Kafka tests."""

    order_id: str


class AcmeSkuProductEvent(LoomFrozenStruct, frozen=True):
    """Sku-only product event shared by compiler and contract multi-source tests."""

    sku: str


class AcmeAmountOrder(LoomStruct):
    """Order with an amount, shared by flow examples and broadcast fixtures."""

    order_id: str
    amount: int


class AcmeChannelOrder(LoomStruct):
    """Order addressed by channel, shared by fork and branch-indexing tests."""

    channel: str
    order_id: str


class AcmeChannelSelector:
    """Selects the channel of an ``AcmeChannelOrder``."""

    def select(self, message: Message[AcmeChannelOrder]) -> object:
        return message.payload.channel


class RecordingSinkPartition:
    """Sink partition that records written batches and can fail a write."""

    def __init__(self, *, fail: bool = False) -> None:
        self.batches: list[Sequence[Any]] = []
        self._fail = fail

    def write_batch(self, items: Sequence[Any]) -> int:
        self.batches.append(list(items))
        if self._fail:
            raise RuntimeError("sink down")
        return len(items)

    def close(self) -> int:
        return 0
