"""Parity contract between poll() and consume_batch() over the typed decode pipeline.

The batch consume path introduced for the partitioned source must be
indistinguishable from the historical poll() path once records reach the
typed (LoomStruct) decode stage.  These tests feed identical broker messages
through both routes and assert byte-, record-, and message-level equality
using the real wire codec — no decode mocking.
"""

from __future__ import annotations

import pytest

from loom.core.model import LoomFrozenStruct
from loom.streaming import ErrorKind
from loom.streaming.core._errors import ErrorEnvelope
from loom.streaming.kafka import (
    ConsumerSettings,
    DecodeError,
    DecodeOk,
    DispatchTable,
    KafkaConsumerClient,
    KafkaRecord,
    MsgspecCodec,
    try_decode_multi_record,
    try_decode_record,
)
from loom.streaming.kafka._message import HEADER_CORRELATION_ID, HEADER_TRACE_ID
from tests.unit.streaming.kafka.cases import OrderCreated, ProductEvent
from tests.unit.streaming.kafka.fakes import (
    ConsumerBackendStub,
    FakeKafkaMessage,
    install_raw_consumer_stub,
)
from tests.unit.streaming.kafka.test_multi_wire import _encode_error, _encode_plain

pytestmark = pytest.mark.kafka

_TOPIC = "orders.events"
_ORDER_MT = "order.created"
_PRODUCT_MT = "product.event"
_SETTINGS = ConsumerSettings(
    brokers=("k1:9092",), group_id="g1", topics=(_TOPIC,), delivery="at_most_once"
)


def _build_consumer(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[KafkaConsumerClient, ConsumerBackendStub]:
    installer = install_raw_consumer_stub(monkeypatch)
    consumer = KafkaConsumerClient(_SETTINGS)
    stub = installer.stub
    assert stub is not None
    return consumer, stub


def _fake_message(value: bytes, *, offset: int) -> FakeKafkaMessage:
    return FakeKafkaMessage(
        topic=_TOPIC,
        key=b"tenant-a",
        value=value,
        headers=[
            (HEADER_TRACE_ID, b"trace-1"),
            (HEADER_CORRELATION_ID, b"corr-1"),
        ],
        partition=2,
        offset=offset,
        timestamp_ms=1000 + offset,
    )


def _consume_via_poll(
    consumer: KafkaConsumerClient,
    stub: ConsumerBackendStub,
    messages: list[FakeKafkaMessage],
) -> list[KafkaRecord[bytes]]:
    records: list[KafkaRecord[bytes]] = []
    for message in messages:
        stub.next_message = message
        record = consumer.poll(10)
        assert record is not None
        records.append(record)
    stub.next_message = None
    return records


def _consume_via_batch(
    consumer: KafkaConsumerClient,
    stub: ConsumerBackendStub,
    messages: list[FakeKafkaMessage],
) -> list[KafkaRecord[bytes]]:
    stub.queued_messages = list(messages)
    return consumer.consume_batch(len(messages))


class TestRecordParity:
    def test_poll_and_consume_batch_return_field_identical_records(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        consumer, stub = _build_consumer(monkeypatch)
        messages = [
            _fake_message(
                _encode_plain(OrderCreated(order_id=f"o-{index}", amount=index), _ORDER_MT),
                offset=index,
            )
            for index in range(3)
        ]

        polled = _consume_via_poll(consumer, stub, messages)
        batched = _consume_via_batch(consumer, stub, messages)

        assert len(polled) == len(batched) == 3
        for record_poll, record_batch in zip(polled, batched, strict=True):
            assert record_poll.topic == record_batch.topic
            assert record_poll.key == record_batch.key
            assert record_poll.value == record_batch.value
            assert record_poll.headers == record_batch.headers
            assert record_poll.partition == record_batch.partition
            assert record_poll.offset == record_batch.offset
            assert record_poll.timestamp_ms == record_batch.timestamp_ms


class TestTypedDecodeParity:
    def test_single_type_decode_produces_identical_messages(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        consumer, stub = _build_consumer(monkeypatch)
        codec = MsgspecCodec[OrderCreated]()
        payload = OrderCreated(order_id="o-1", amount=5)
        messages = [_fake_message(_encode_plain(payload, _ORDER_MT), offset=9)]

        polled = _consume_via_poll(consumer, stub, messages)
        batched = _consume_via_batch(consumer, stub, messages)

        result_poll = try_decode_record(polled[0], OrderCreated, codec)
        result_batch = try_decode_record(batched[0], OrderCreated, codec)

        assert isinstance(result_poll, DecodeOk)
        assert isinstance(result_batch, DecodeOk)
        assert result_poll.message == result_batch.message
        message = result_batch.message
        assert message.payload == payload
        assert message.meta.partition == 2
        assert message.meta.offset == 9
        assert message.meta.headers[HEADER_TRACE_ID] == b"trace-1"
        assert message.meta.headers[HEADER_CORRELATION_ID] == b"corr-1"
        assert result_poll.message.meta.headers == result_batch.message.meta.headers


class TestMultiTypeDecodeParity:
    def test_mixed_types_dispatch_identically_batch_and_one_by_one(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        consumer, stub = _build_consumer(monkeypatch)
        dispatch = DispatchTable(
            plain={_ORDER_MT: OrderCreated, _PRODUCT_MT: ProductEvent},
            error={
                _ORDER_MT: ErrorEnvelope[OrderCreated],
                _PRODUCT_MT: ErrorEnvelope[ProductEvent],
            },
            wire={DecodeError.loom_message_type(): DecodeError},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        wire_payload = DecodeError(
            error=ErrorEnvelope(kind=ErrorKind.WIRE, reason="wire-failed"),
            raw=b"bad-wire",
            topic=_TOPIC,
            key=b"k",
            headers={},
        )
        messages = [
            _fake_message(
                _encode_plain(OrderCreated(order_id="o-1", amount=1), _ORDER_MT), offset=1
            ),
            _fake_message(_encode_plain(ProductEvent(sku="sku-1", stock=3), _PRODUCT_MT), offset=2),
            _fake_message(
                _encode_error(OrderCreated(order_id="o-2", amount=2), _ORDER_MT), offset=3
            ),
            _fake_message(_encode_plain(wire_payload, DecodeError.loom_message_type()), offset=4),
        ]

        polled = _consume_via_poll(consumer, stub, messages)
        batched = _consume_via_batch(consumer, stub, messages)

        results_poll = [try_decode_multi_record(record, dispatch, codec) for record in polled]
        results_batch = [try_decode_multi_record(record, dispatch, codec) for record in batched]

        assert results_poll == results_batch
        assert all(isinstance(result, DecodeOk) for result in results_batch)
        decoded = [
            result.message.payload for result in results_batch if isinstance(result, DecodeOk)
        ]
        assert isinstance(decoded[0], OrderCreated)
        assert isinstance(decoded[1], ProductEvent)
        assert isinstance(decoded[2], ErrorEnvelope)
        assert decoded[2].payload_type == _ORDER_MT
        assert isinstance(decoded[3], DecodeError)
        assert decoded[3].error.reason == "wire-failed"


class TestWireErrorParity:
    def test_corrupt_bytes_produce_identical_decode_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        consumer, stub = _build_consumer(monkeypatch)
        codec = MsgspecCodec[OrderCreated]()
        messages = [_fake_message(b"not-msgpack", offset=7)]

        polled = _consume_via_poll(consumer, stub, messages)
        batched = _consume_via_batch(consumer, stub, messages)

        result_poll = try_decode_record(polled[0], OrderCreated, codec)
        result_batch = try_decode_record(batched[0], OrderCreated, codec)

        assert isinstance(result_poll, DecodeError)
        assert isinstance(result_batch, DecodeError)
        assert result_poll == result_batch
        assert result_batch.error.kind is ErrorKind.WIRE
        assert result_batch.raw == b"not-msgpack"
        assert result_batch.topic == _TOPIC
        assert result_batch.partition == 2
        assert result_batch.offset == 7
