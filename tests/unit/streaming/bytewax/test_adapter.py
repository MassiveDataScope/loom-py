"""TDD for Bytewax runtime adapter — end-to-end execution."""

from __future__ import annotations

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.observability.observability import Observability  # noqa: E402
from loom.streaming.bytewax import _adapter  # noqa: E402
from loom.streaming.compiler import CompiledSink  # noqa: E402
from loom.streaming.core._errors import ErrorKind  # noqa: E402
from loom.streaming.core._message import Message  # noqa: E402
from loom.streaming.kafka import (  # noqa: E402
    DecodeError,
    KafkaRecord,
    MessageDescriptor,
    MsgspecCodec,
    build_message,
)
from loom.streaming.kafka._config import ProducerSettings  # noqa: E402
from loom.streaming.nodes._boundary import IntoTopic  # noqa: E402
from loom.streaming.nodes._shape import Drain  # noqa: E402
from loom.streaming.testing import StreamingTestRunner  # noqa: E402
from tests.unit.streaming.bytewax.cases import (  # noqa: E402
    AcmeOrder,
    AcmeResult,
    DoubleStep,
    SuffixStep,
    build_compiled_plan,
)

pytestmark = pytest.mark.bytewax


class TestStepNodeExecution:
    """A simple Step node must transform records end-to-end."""

    def test_single_task_produces_transformed_output(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_message: Message[AcmeOrder],
    ) -> None:
        plan = build_compiled_plan(bytewax_double_step)
        runner = StreamingTestRunner(plan).with_messages([bytewax_message])
        runner.run()
        results = runner.output

        assert len(results) == 1
        assert isinstance(results[0], Message)
        assert results[0].payload == AcmeResult(value="AA")
        assert results[0].meta.message_id == "msg-1"

    def test_task_chain_passes_message_to_next_task(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_suffix_step: SuffixStep,
        bytewax_message: Message[AcmeOrder],
    ) -> None:
        plan = build_compiled_plan(bytewax_double_step, bytewax_suffix_step)
        runner = StreamingTestRunner(plan).with_messages([bytewax_message])
        runner.run()
        results = runner.output

        assert [message.payload.value for message in results] == ["AA:ok"]


class TestSourceDecode:
    """Raw Kafka records must be decoded before reaching DSL nodes."""

    def test_kafka_record_source_decodes_envelope_before_task_execution(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_order: AcmeOrder,
    ) -> None:
        plan = build_compiled_plan(bytewax_double_step)
        codec = MsgspecCodec[AcmeOrder]()
        envelope = build_message(
            bytewax_order,
            MessageDescriptor(message_type="order.created", message_version=1),
            correlation_id="corr-1",
            trace_id="trace-1",
            produced_at_ms=10,
        )
        source_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=codec.encode(envelope),
            headers={"h": b"1"},
            partition=0,
            offset=3,
            timestamp_ms=11,
        )
        runner = StreamingTestRunner(plan).with_messages([source_record])
        runner.run()
        results = runner.output

        assert len(results) == 1
        assert results[0].payload == AcmeResult(value="AA")
        assert results[0].meta.message_id == "orders.in:0:3"
        assert results[0].meta.message_type == "order.created"
        assert results[0].meta.message_version == 1
        assert results[0].meta.correlation_id == "corr-1"
        assert results[0].meta.trace_id == "trace-1"
        assert results[0].meta.produced_at_ms == 10
        assert results[0].meta.key == b"tenant-a"
        assert results[0].meta.headers == {"h": b"1"}

    def test_wire_decode_error_routes_to_testing_error_sink(
        self,
        bytewax_double_step: DoubleStep,
    ) -> None:
        error_sink = CompiledSink(
            settings=ProducerSettings(
                brokers=("localhost:9092",),
                client_id="test-producer",
                topic="orders.dlq",
            ),
            topic="orders.dlq",
            partition_policy=None,
        )
        plan = build_compiled_plan(bytewax_double_step, error_routes={ErrorKind.WIRE: error_sink})
        source_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=b"not-msgpack",
            headers={"h": b"1"},
            partition=0,
            offset=4,
            timestamp_ms=12,
        )
        runner = (
            StreamingTestRunner(plan).with_messages([source_record]).capture_errors(ErrorKind.WIRE)
        )
        runner.run()
        results = runner.output
        errors = runner.errors[ErrorKind.WIRE]

        assert results == []
        assert len(errors) == 1
        assert isinstance(errors[0], DecodeError)
        assert errors[0].error.kind is ErrorKind.WIRE
        assert errors[0].error.original_message is None
        assert errors[0].raw == b"not-msgpack"
        assert errors[0].topic == "orders.in"
        assert errors[0].partition == 0
        assert errors[0].offset == 4
        assert errors[0].key == b"tenant-a"
        assert errors[0].headers == {"h": b"1"}


class TestTerminalNodes:
    """Terminal DSL nodes must be executable with Bytewax testing sinks."""

    def test_into_topic_node_wires_terminal_sink(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_message: Message[AcmeOrder],
    ) -> None:
        target = IntoTopic("out", payload=AcmeResult)
        plan = build_compiled_plan(bytewax_double_step, output=target)
        runner = StreamingTestRunner(plan).with_messages([bytewax_message])
        runner.run()
        results = runner.output

        assert [message.payload.value for message in results] == ["AA"]

    def test_drain_node_swallows_stream(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_message: Message[AcmeOrder],
    ) -> None:
        plan = build_compiled_plan(bytewax_double_step, Drain())
        runner = StreamingTestRunner(plan).with_messages([bytewax_message])
        runner.run()
        results = runner.output

        assert results == []

    def test_payload_helper_builds_default_test_metadata(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_order: AcmeOrder,
    ) -> None:
        plan = build_compiled_plan(bytewax_double_step)
        runner = StreamingTestRunner(plan).with_payloads([bytewax_order])

        runner.run()

        results = runner.output
        assert len(results) == 1
        assert results[0].meta.message_id == "test-0"
        assert results[0].meta.topic == "in"

    def test_into_topic_node_does_not_duplicate_flow_output(
        self,
        bytewax_double_step: DoubleStep,
        bytewax_message: Message[AcmeOrder],
    ) -> None:
        target = IntoTopic("out", payload=AcmeResult)
        plan = build_compiled_plan(bytewax_double_step, output=target)
        runner = StreamingTestRunner(plan).with_messages([bytewax_message])

        runner.run()

        results = runner.output
        assert len(results) == 1, "IntoTopic output must not be counted twice"
        assert results[0].payload.value == "AA"


class TestOutputAndErrorWiring:
    """Output and error routes must coexist without interfering."""

    def test_valid_output_and_wire_error_can_be_captured_together(
        self,
        bytewax_double_step: DoubleStep,
    ) -> None:
        error_sink = CompiledSink(
            settings=ProducerSettings(
                brokers=("localhost:9092",),
                client_id="test-producer",
                topic="orders.dlq",
            ),
            topic="orders.dlq",
            partition_policy=None,
        )
        target = IntoTopic("out", payload=AcmeResult)
        codec = MsgspecCodec[AcmeOrder]()
        envelope = build_message(
            AcmeOrder(order_id="A"),
            MessageDescriptor(message_type="order.created", message_version=1),
        )
        valid_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=codec.encode(envelope),
            headers={},
            partition=0,
            offset=1,
            timestamp_ms=10,
        )
        invalid_record = KafkaRecord(
            topic="orders.in",
            key=b"tenant-a",
            value=b"bad-wire",
            headers={},
            partition=0,
            offset=2,
            timestamp_ms=11,
        )
        plan = build_compiled_plan(
            bytewax_double_step,
            output=target,
            error_routes={ErrorKind.WIRE: error_sink},
        )
        runner = (
            StreamingTestRunner(plan)
            .with_messages([valid_record, invalid_record])
            .capture_errors(ErrorKind.WIRE)
        )

        runner.run()

        assert [message.payload.value for message in runner.output] == ["AA"]
        errors = runner.errors[ErrorKind.WIRE]
        assert len(errors) == 1
        assert errors[0].raw == b"bad-wire"
        assert errors[0].offset == 2

    def test_wire_decode_error_without_explicit_sink_uses_drop_sink(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        recorded: dict[str, object] = {}

        def _bw_output(step_id: str, stream: object, sink: object) -> None:
            recorded["step_id"] = step_id
            recorded["stream"] = stream
            recorded["sink_type"] = type(sink).__name__

        monkeypatch.setattr(_adapter, "bw_output", _bw_output)

        ctx = _adapter._BuildContext(
            plan=build_compiled_plan(),
            bridge=None,
            flow_runtime=Observability.noop(),
            flow_run_id="run-1",
            sink=None,
            terminal_sinks={},
            error_sinks={},
        )
        ctx.wire_node_error(ErrorKind.WIRE, "decode", "stream")

        assert recorded["step_id"] == "decode_wire_dropped"
        assert recorded["stream"] == "stream"
        assert recorded["sink_type"] == "_DropSink"


class TestDropSinkCompletion:
    def test_drop_sink_completes_every_droppable_item_kind(self) -> None:
        """A drop that never completes freezes the watermark under at_least_once."""
        from loom.streaming.bytewax._adapter import _DropSinkPartition
        from loom.streaming.core._errors import (
            ErrorEnvelope,
            ErrorKind,
            ErrorMessage,
            ErrorMessageMeta,
        )
        from loom.streaming.kafka._wire import DecodeError
        from tests.unit.streaming.bytewax.cases import AcmeOrder, build_order_message

        completed: list[tuple[str, int, int]] = []

        class _Tracker:
            """The whole of ``CommitCompletionPort``, so the stub is checkable.

            ``fork`` is part of the port a drop sink is handed; leaving it out
            made the stub structurally not a ``CommitCompletionPort``, which
            only surfaced once the type checker started reading tests.
            """

            def fork(self, topic: str, partition: int, offset: int, extra_outputs: int) -> None:
                raise AssertionError("a drop sink must not fork a record")

            def complete(self, topic: str, partition: int, offset: int) -> None:
                completed.append((topic, partition, offset))

        message = build_order_message("1", None, partition=0, offset=7)
        envelope: ErrorEnvelope[AcmeOrder] = ErrorEnvelope(
            kind=ErrorKind.BUSINESS,
            reason="boom",
            original_message=ErrorMessage(
                payload=AcmeOrder(order_id="2"),
                meta=ErrorMessageMeta(message_id="m2", topic="orders.in", partition=1, offset=8),
            ),
        )
        decode_error = DecodeError(
            error=ErrorEnvelope(kind=ErrorKind.WIRE, reason="bad-bytes"),
            raw=b"x",
            topic="orders.in",
            key=None,
            headers={},
            partition=2,
            offset=9,
            timestamp_ms=None,
        )

        partition = _DropSinkPartition(_Tracker())
        partition.write_batch([message, envelope, decode_error])

        assert completed == [
            ("orders.in", 0, 7),
            ("orders.in", 1, 8),
            ("orders.in", 2, 9),
        ]
