"""Multi-type dispatch wire decode tests."""

from __future__ import annotations

import pytest

from loom.core.model import LoomFrozenStruct
from loom.streaming import ErrorKind
from loom.streaming.core import DispatchTable
from loom.streaming.core._errors import ErrorEnvelope, ErrorMessage, ErrorMessageMeta
from loom.streaming.kafka import (
    DecodeError,
    DecodeOk,
    KafkaRecord,
    MessageDescriptor,
    MsgspecCodec,
    build_message,
    try_decode_multi_record,
)
from tests.unit.streaming.doubles import AcmeOrderEvent

pytestmark = pytest.mark.kafka


class AcmeProductEvent(LoomFrozenStruct, frozen=True):
    sku: str
    stock: int


_ORDER_MT = "order.event"
_PRODUCT_MT = "product.event"
_ERROR_MT_TASK = "loom.streaming.error.task"


def _encode_plain(payload: LoomFrozenStruct, message_type: str) -> bytes:
    codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
    descriptor = MessageDescriptor(message_type=message_type, message_version=1)
    envelope = build_message(payload, descriptor, produced_at_ms=1000)
    return codec.encode(envelope)


def _encode_error(
    payload: LoomFrozenStruct,
    inner_message_type: str,
    kind: ErrorKind = ErrorKind.TASK,
) -> bytes:
    meta = ErrorMessageMeta(message_id="src-1", message_type=inner_message_type)
    original = ErrorMessage(payload=payload, meta=meta)
    error_envelope = ErrorEnvelope(
        kind=kind,
        reason="processing failed",
        payload_type=inner_message_type,
        original_message=original,
    )
    codec: MsgspecCodec[ErrorEnvelope[LoomFrozenStruct]] = MsgspecCodec()
    descriptor = MessageDescriptor(
        message_type=f"loom.streaming.error.{kind.value}",
        message_version=1,
    )
    envelope = build_message(error_envelope, descriptor, produced_at_ms=1000)
    return codec.encode(envelope)


def _record(value: bytes, topic: str = "events.all") -> KafkaRecord[bytes]:
    return KafkaRecord(topic=topic, key=b"k", value=value, partition=0, offset=1)


class TestTryDecodeMultiRecord:
    @pytest.mark.parametrize(
        ("payload", "message_type", "expected_type", "expected_field", "expected_value"),
        [
            (
                AcmeOrderEvent(order_id="o-1"),
                _ORDER_MT,
                AcmeOrderEvent,
                "order_id",
                "o-1",
            ),
            (
                AcmeProductEvent(sku="sku-1", stock=10),
                _PRODUCT_MT,
                AcmeProductEvent,
                "sku",
                "sku-1",
            ),
        ],
    )
    def test_dispatches_plain_types_by_message_type(
        self,
        payload: LoomFrozenStruct,
        message_type: str,
        expected_type: type[LoomFrozenStruct],
        expected_field: str,
        expected_value: str,
    ) -> None:
        dispatch = DispatchTable(
            plain={_ORDER_MT: AcmeOrderEvent, _PRODUCT_MT: AcmeProductEvent},
            error={},
            wire={},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        record = _record(_encode_plain(payload, message_type))

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeOk)
        assert isinstance(result.message.payload, expected_type)
        assert getattr(result.message.payload, expected_field) == expected_value

    @pytest.mark.parametrize(
        ("record", "expected_reason"),
        [
            (
                _record(_encode_plain(AcmeProductEvent(sku="sku-1", stock=5), _PRODUCT_MT)),
                _PRODUCT_MT,
            ),
            (
                _record(b"not-msgpack"),
                "failed to probe message_type from envelope",
            ),
        ],
    )
    def test_wire_errors_return_decode_error(
        self,
        record: KafkaRecord[bytes],
        expected_reason: str,
    ) -> None:
        dispatch = DispatchTable(plain={_ORDER_MT: AcmeOrderEvent}, error={}, wire={})
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeError)
        assert result.error.kind is ErrorKind.WIRE
        assert expected_reason in result.error.reason

    def test_wire_errors_log_a_warning(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _LoggerProbe:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            def warning(self, event: str, **fields: object) -> None:
                self.calls.append(("warning", event))

        logger = _LoggerProbe()
        monkeypatch.setattr("loom.streaming.kafka._wire.logger", logger)
        dispatch = DispatchTable(plain={_ORDER_MT: AcmeOrderEvent}, error={}, wire={})
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()

        result = try_decode_multi_record(_record(b"not-msgpack"), dispatch, codec)

        assert isinstance(result, DecodeError)
        assert ("warning", "multi_source_wire_error") in logger.calls

    @pytest.mark.parametrize(
        ("payload", "inner_message_type"),
        [
            (AcmeOrderEvent(order_id="o-2"), _ORDER_MT),
            (AcmeProductEvent(sku="s-1", stock=3), _PRODUCT_MT),
        ],
    )
    def test_dispatches_error_envelope_by_inner_payload_type(
        self,
        payload: LoomFrozenStruct,
        inner_message_type: str,
    ) -> None:
        dispatch = DispatchTable(
            plain={},
            error={
                _ORDER_MT: ErrorEnvelope[AcmeOrderEvent],
                _PRODUCT_MT: ErrorEnvelope[AcmeProductEvent],
            },
            wire={},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()

        record = _record(_encode_error(payload, inner_message_type), topic="errors.all")

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeOk)
        assert isinstance(result.message.payload, ErrorEnvelope)
        assert result.message.payload.payload_type == inner_message_type
        assert result.message.payload.kind is ErrorKind.TASK

    def test_error_envelope_with_unknown_inner_type_returns_wire_error(self) -> None:
        dispatch = DispatchTable(
            plain={},
            error={_ORDER_MT: ErrorEnvelope[AcmeOrderEvent]},
            wire={},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        record = _record(
            _encode_error(AcmeProductEvent(sku="s-1", stock=1), _PRODUCT_MT), topic="errors.all"
        )

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeError)
        assert result.error.kind is ErrorKind.WIRE

    @pytest.mark.parametrize(
        ("record", "expect_ok"),
        [
            (
                KafkaRecord(
                    topic="events.all",
                    key=b"tenant-1",
                    value=_encode_plain(AcmeOrderEvent(order_id="o-4"), _ORDER_MT),
                    partition=2,
                    offset=5,
                    timestamp_ms=9999,
                ),
                True,
            ),
            (
                KafkaRecord(
                    topic="events.all",
                    key="k",
                    value=b"garbage",
                    partition=1,
                    offset=7,
                    timestamp_ms=111,
                ),
                False,
            ),
        ],
    )
    def test_preserves_record_metadata_on_decode_results(
        self,
        record: KafkaRecord[bytes],
        expect_ok: bool,
    ) -> None:
        dispatch = DispatchTable(plain={_ORDER_MT: AcmeOrderEvent}, error={}, wire={})
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()

        result = try_decode_multi_record(record, dispatch, codec)

        if expect_ok:
            assert isinstance(result, DecodeOk)
            assert result.message.meta.topic == "events.all"
            assert result.message.meta.partition == 2
            assert result.message.meta.offset == 5
            assert result.message.meta.key == b"tenant-1"
            return

        assert isinstance(result, DecodeError)
        assert result.raw == b"garbage"
        assert result.topic == "events.all"
        assert result.partition == 1
        assert result.offset == 7
        assert result.key == b"k"

    def test_dispatches_wire_decode_error_by_message_type(self) -> None:
        dispatch = DispatchTable(
            plain={_ORDER_MT: AcmeOrderEvent},
            error={},
            wire={DecodeError.loom_message_type(): DecodeError},
        )
        codec: MsgspecCodec[LoomFrozenStruct] = MsgspecCodec()
        error = DecodeError(
            error=ErrorEnvelope(kind=ErrorKind.WIRE, reason="wire-failed"),
            raw=b"bad-wire",
            topic="errors.all",
            key=b"k",
            headers={},
            partition=3,
            offset=9,
            timestamp_ms=222,
        )
        record = _record(
            _encode_plain(error, DecodeError.loom_message_type()),
            topic="errors.all",
        )

        result = try_decode_multi_record(record, dispatch, codec)

        assert isinstance(result, DecodeOk)
        assert isinstance(result.message.payload, DecodeError)
        assert result.message.payload.error.kind is ErrorKind.WIRE
        assert result.message.payload.error.reason == "wire-failed"
