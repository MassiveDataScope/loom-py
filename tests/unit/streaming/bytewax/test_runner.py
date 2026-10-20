from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar

import msgspec
import pytest
import uvloop

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from bytewax.dataflow import Dataflow  # noqa: E402
from opentelemetry import trace  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)

from loom.core.async_bridge import build_backend_options as _build_backend_options  # noqa: E402
from loom.core.config import ConfigContext  # noqa: E402
from loom.core.observability.event import (  # noqa: E402
    EventKind,
    LifecycleEvent,
    LifecycleStatus,
    Scope,
)
from loom.core.observability.runtime import ObservabilityRuntime  # noqa: E402
from loom.streaming import Drain, FromMongoCDC, Process, StreamFlow  # noqa: E402
from loom.streaming.bytewax.runner import (  # noqa: E402
    BytewaxRuntimeConfig,
    StreamingRunner,
)
from loom.streaming.core._errors import ErrorKind  # noqa: E402
from loom.streaming.mongo import MongoCDCEvent  # noqa: E402
from tests.unit.streaming.bytewax.cases import Order, Result  # noqa: E402

pytestmark = pytest.mark.bytewax


class TestStreamingRunner:
    def test_run_generates_a_poll_cycle_id(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        runtime = ObservabilityRuntime([_RecordingObserver()])
        runner = StreamingRunner.from_dict(
            bytewax_stream_flow,
            bytewax_runtime_config_dict,
            observability_runtime=runtime,
        )
        dataflow = Dataflow("test")

        def _fake_prepare(**kwargs: object) -> object:
            def shutdown() -> None:
                return None

            return SimpleNamespace(
                dataflow=dataflow,
                shutdown=shutdown,
            )

        monkeypatch.setattr(runner, "prepare_run", _fake_prepare)
        monkeypatch.setattr(
            "loom.streaming.bytewax.runner.cli_main", lambda *_args, **_kwargs: None
        )
        monkeypatch.setattr("loom.streaming.bytewax.runner.generate_trace_id", lambda: "poll-trace")

        runner.run()

        poll_cycle = [e for e in events if e.scope == Scope.POLL_CYCLE]
        assert [e.kind for e in poll_cycle] == [EventKind.START, EventKind.END]
        assert poll_cycle[0].id == "poll-trace"
        assert poll_cycle[1].id == "poll-trace"
        assert poll_cycle[0].trace_id is None
        assert poll_cycle[1].trace_id is None

    def test_run_uses_bytewax_cli_main_with_runtime_config(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)
        dataflow = Dataflow("test")
        shutdown_calls: list[str] = []
        cli_calls: dict[str, object] = {}

        def _fake_prepare(**kwargs: object) -> object:
            def shutdown() -> None:
                shutdown_calls.append("done")

            runner._shutdown = shutdown
            return SimpleNamespace(
                dataflow=dataflow,
                shutdown=shutdown,
            )

        def _fake_cli_main(flow: Dataflow, **kwargs: object) -> None:
            cli_calls["flow"] = flow
            cli_calls["kwargs"] = kwargs

        monkeypatch.setattr(runner, "prepare_run", _fake_prepare)
        monkeypatch.setattr("loom.streaming.bytewax.runner.cli_main", _fake_cli_main)

        runner.run()

        assert cli_calls["flow"] is dataflow
        kwargs = cli_calls["kwargs"]
        assert isinstance(kwargs, dict)
        assert kwargs["workers_per_process"] == 2
        assert kwargs["process_id"] == 1
        assert kwargs["addresses"] == ["127.0.0.1:2101", "127.0.0.1:2102"]
        assert shutdown_calls == ["done"]

    def test_run_calls_shutdown_when_cli_main_raises(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)
        dataflow = Dataflow("test")
        shutdown_calls: list[str] = []

        def _fake_prepare(**kwargs: object) -> object:
            def shutdown() -> None:
                shutdown_calls.append("done")

            runner._shutdown = shutdown
            return SimpleNamespace(dataflow=dataflow, shutdown=shutdown)

        def _fake_cli_main(flow: Dataflow, **kwargs: object) -> None:
            raise RuntimeError("boom")

        monkeypatch.setattr(runner, "prepare_run", _fake_prepare)
        monkeypatch.setattr("loom.streaming.bytewax.runner.cli_main", _fake_cli_main)

        with pytest.raises(RuntimeError, match="boom"):
            runner.run()

        assert shutdown_calls == ["done"]

    def test_from_context_loads_runtime_section(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
    ) -> None:
        runner = StreamingRunner.from_context(
            bytewax_stream_flow,
            config=ConfigContext.from_dict(bytewax_runtime_config_dict),
        )

        assert runner._runtime.workers_per_process == 2
        assert runner._runtime.process_id == 1
        assert runner._runtime.addresses == ("127.0.0.1:2101", "127.0.0.1:2102")
        assert runner._runtime.epoch_interval_ms == 5000
        assert runner._runtime.recovery is not None
        assert runner._runtime.recovery.db_dir == "/var/lib/loom/tests/bytewax-recovery"
        assert runner._runtime.recovery.backup_interval_ms == 30000

    def test_from_dict_loads_streaming_observability_section(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``endpoint=""`` wires the host's provider, so its spans are exported."""
        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        monkeypatch.setattr(trace, "_TRACER_PROVIDER", provider, raising=False)

        with runner._observability_runtime.span(Scope.POLL_CYCLE, "orders"):
            pass

        assert [span.name for span in exporter.get_finished_spans()] == ["poll_cycle:orders"]

    def test_from_yaml_loads_runtime_section(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "streaming.yaml"
        config_path.write_text(
            """
kafka:
  consumer:
    brokers: ["localhost:9092"]
    group_id: "test"
    delivery: at_most_once
    topics: ["orders.in"]
  producer:
    brokers: ["localhost:9092"]
    client_id: "test-producer"
    topic: "orders.out"
streaming:
  runtime:
    workers_per_process: 3
    epoch_interval_ms: 7000
    process_id: 2
    addresses: ["127.0.0.1:2201", "127.0.0.1:2202"]
    recovery:
      db_dir: "/var/lib/loom/tests/bytewax-runtime"
      backup_interval_ms: 45000
""".strip()
        )

        runner = StreamingRunner.from_yaml(bytewax_stream_flow, str(config_path))

        assert runner._runtime.workers_per_process == 3
        assert runner._runtime.process_id == 2
        assert runner._runtime.addresses == ("127.0.0.1:2201", "127.0.0.1:2202")
        assert runner._runtime.epoch_interval_ms == 7000
        assert runner._runtime.recovery is not None
        assert runner._runtime.recovery.db_dir == "/var/lib/loom/tests/bytewax-runtime"
        assert runner._runtime.recovery.backup_interval_ms == 45000


class TestPrepareRun:
    def test_build_dataflow_supports_mongo_cdc_source_without_runtime_import(
        self,
    ) -> None:
        flow: StreamFlow[MongoCDCEvent, MongoCDCEvent] = StreamFlow(
            name="mongo_runner_build",
            source=FromMongoCDC("domain_events", collections=("orders",)),
            process=Process(Drain()),
        )
        runner = StreamingRunner.from_dict(flow, _mongo_runner_config())

        dataflow = runner.build_dataflow()

        assert isinstance(dataflow, Dataflow)
        runner.shutdown()

    def test_prepare_run_releases_previous_shutdown_before_rebuilding(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)
        shutdown_calls: list[str] = []
        call_count = 0

        def _fake_prepare_run(
            plan: object,
            *,
            observability_runtime: object = None,
            runtime: object = None,
            **kwargs: object,
        ) -> object:
            nonlocal call_count
            call_count += 1
            n = call_count

            def shutdown() -> None:
                shutdown_calls.append(f"run-{n}")

            return SimpleNamespace(dataflow=Dataflow("test"), shutdown=shutdown)

        monkeypatch.setattr("loom.streaming.bytewax.runner._prepare_run", _fake_prepare_run)

        runner.prepare_run()
        assert shutdown_calls == []

        runner.prepare_run()
        assert shutdown_calls == ["run-1"]


class TestBytewaxRuntimeConfig:
    def test_defaults_match_runtime_contract(self) -> None:
        cfg = BytewaxRuntimeConfig()

        assert cfg.async_backend == "asyncio"
        assert cfg.use_uvloop is False
        assert cfg.force_shutdown_timeout_ms is None

    def test_custom_values_are_preserved(self) -> None:
        cfg = BytewaxRuntimeConfig(
            async_backend="trio",
            use_uvloop=True,
            force_shutdown_timeout_ms=5000,
        )

        assert cfg.async_backend == "trio"
        assert cfg.use_uvloop is True
        assert cfg.force_shutdown_timeout_ms == 5000


class TestBuildBackendOptions:
    def test_asyncio_without_uvloop_returns_empty(self) -> None:
        assert _build_backend_options("asyncio", use_uvloop=False) == {}

    def test_trio_with_uvloop_flag_returns_empty(self) -> None:
        assert _build_backend_options("trio", use_uvloop=True) == {}

    def test_asyncio_with_uvloop_adds_loop_factory(self) -> None:
        if sys.platform == "win32":
            pytest.skip("uvloop not available on Windows")

        opts = _build_backend_options("asyncio", use_uvloop=True)

        assert opts.get("loop_factory") is uvloop.new_event_loop


class TestRunFlowSpan:
    def test_run_emits_flow_start_and_end(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        runtime = ObservabilityRuntime([_RecordingObserver()])
        runner = StreamingRunner.from_dict(
            bytewax_stream_flow,
            bytewax_runtime_config_dict,
            observability_runtime=runtime,
        )
        dataflow = Dataflow("test")

        def _fake_prepare(**kwargs: object) -> object:
            return SimpleNamespace(dataflow=dataflow, shutdown=lambda: None)

        monkeypatch.setattr(runner, "prepare_run", _fake_prepare)
        monkeypatch.setattr("loom.streaming.bytewax.runner.cli_main", lambda *_a, **_kw: None)

        runner.run()

        flow_events = [e for e in events if e.scope == Scope.FLOW]
        assert [e.kind for e in flow_events] == [EventKind.START, EventKind.END]
        assert flow_events[1].duration_ms is not None
        assert "node_count" in flow_events[0].meta
        assert flow_events[0].meta["node_count"] == flow_events[1].meta["node_count"]

    def test_run_emits_flow_error_when_cli_main_raises(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        runtime = ObservabilityRuntime([_RecordingObserver()])
        runner = StreamingRunner.from_dict(
            bytewax_stream_flow,
            bytewax_runtime_config_dict,
            observability_runtime=runtime,
        )
        dataflow = Dataflow("test")

        def _fake_prepare(**kwargs: object) -> object:
            return SimpleNamespace(dataflow=dataflow, shutdown=lambda: None)

        def _raise_stream_failure(*_a: object, **_kw: object) -> None:
            raise RuntimeError("stream failure")

        monkeypatch.setattr(runner, "prepare_run", _fake_prepare)
        monkeypatch.setattr("loom.streaming.bytewax.runner.cli_main", _raise_stream_failure)

        with pytest.raises(RuntimeError, match="stream failure"):
            runner.run()

        flow_events = [e for e in events if e.scope == Scope.FLOW]
        assert flow_events[0].kind is EventKind.START
        assert flow_events[1].kind is EventKind.ERROR

    def test_run_emits_poll_cycle_end_when_prepare_run_fails(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        runtime = ObservabilityRuntime([_RecordingObserver()])
        runner = StreamingRunner.from_dict(
            bytewax_stream_flow,
            bytewax_runtime_config_dict,
            observability_runtime=runtime,
        )

        def _raise_prep_failed(**kwargs: object) -> None:
            raise RuntimeError("prep failed")

        monkeypatch.setattr(runner, "prepare_run", _raise_prep_failed)

        with pytest.raises(RuntimeError, match="prep failed"):
            runner.run()

        poll_cycle = [e for e in events if e.scope == Scope.POLL_CYCLE]
        assert poll_cycle[0].kind is EventKind.START
        # The run is one span now, so a failure closes it as ERROR rather than
        # as an END carrying a FAILURE status. Prometheus counts it, and the
        # OTEL span carries the exception.
        assert poll_cycle[1].kind is EventKind.ERROR
        assert poll_cycle[1].status is LifecycleStatus.FAILURE
        assert poll_cycle[1].error == "prep failed"


class TestPrepareRunErrorSinks:
    def test_prepare_run_accepts_error_sinks_and_passes_them_through(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from loom.streaming.bytewax.runner import ErrorSink
        from loom.streaming.core._errors import ErrorKind

        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)
        received_kwargs: dict[str, object] = {}

        class _StubErrorSink:
            def build(self, step_id: str, worker_index: int, worker_count: int) -> object:
                return object()

        error_sink_value: ErrorSink = _StubErrorSink()

        def _fake_prepare_run(
            plan: object,
            *,
            observability_runtime: object = None,
            runtime: object = None,
            **kwargs: object,
        ) -> object:
            received_kwargs.update(kwargs)
            return SimpleNamespace(dataflow=Dataflow("test"), shutdown=lambda: None)

        monkeypatch.setattr("loom.streaming.bytewax.runner._prepare_run", _fake_prepare_run)

        runner.prepare_run(error_sinks={ErrorKind.TASK: error_sink_value})

        assert "error_sinks" in received_kwargs
        assert received_kwargs["error_sinks"] == {ErrorKind.TASK: error_sink_value}


def _mongo_runner_config() -> dict[str, object]:
    return {
        "mongo": {
            "sources": {
                "domain_events": {
                    "uri": "mongodb://localhost:27017",
                    "database": "app",
                }
            }
        }
    }


# ---------------------------------------------------------------------------
# Fake helpers for TestRegisterSink
# ---------------------------------------------------------------------------


class _RegistryFakeConfig(msgspec.Struct, frozen=True):
    some_field: str = "default"


class _RegistryFakeSink:
    """Minimal sink satisfying the RegisteredSink protocol."""

    sink_type: ClassVar[str] = "registry_test_sink"
    config_type: ClassVar[type] = _RegistryFakeConfig

    @classmethod
    def build_binding(cls, cfg: Any, ctx: ConfigContext) -> Any:
        from loom.streaming.bytewax._sink_registry import RuntimeSinkBinding

        return RuntimeSinkBinding(
            purpose="errors",
            sink=object(),
            kinds=(ErrorKind.TASK,),
        )


# ---------------------------------------------------------------------------
# TestRegisterSink
# ---------------------------------------------------------------------------


class TestRegisterSink:
    def test_register_sink_stores_in_registry(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
    ) -> None:
        """AC-1 at runner level: register_sink delegates to the internal SinkRegistry."""
        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)

        runner.register_sink(_RegistryFakeSink)

        # The runner must expose its registry so we can inspect the stored entry.
        assert _RegistryFakeSink in runner._registry

    def test_run_with_config_path_resolves_registered_sinks(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """AC-2: when a YAML config path is given, resolved sinks are passed to _prepare_run."""
        config_path = tmp_path / "streaming.yaml"
        config_path.write_text(
            """
kafka:
  consumer:
    brokers: ["localhost:9092"]
    group_id: "test"
    delivery: at_most_once
    topics: ["orders.in"]
  producer:
    brokers: ["localhost:9092"]
    client_id: "test-producer"
    topic: "orders.out"
streaming:
  sinks:
    my_sink:
      type: registry_test_sink
      some_field: value
""".strip()
        )

        runner = StreamingRunner.from_yaml(bytewax_stream_flow, str(config_path))
        runner.register_sink(_RegistryFakeSink)

        received_kwargs: dict[str, object] = {}

        def _fake_prepare_run(
            plan: object,
            *,
            observability_runtime: object = None,
            runtime: object = None,
            **kwargs: object,
        ) -> object:
            received_kwargs.update(kwargs)
            return SimpleNamespace(dataflow=Dataflow("test"), shutdown=lambda: None)

        monkeypatch.setattr("loom.streaming.bytewax.runner._prepare_run", _fake_prepare_run)
        monkeypatch.setattr("loom.streaming.bytewax.runner.cli_main", lambda *_a, **_kw: None)

        runner.run(config_path=str(config_path))

        assert "error_sinks" in received_kwargs
        error_sinks = received_kwargs["error_sinks"]
        assert isinstance(error_sinks, dict)
        assert ErrorKind.TASK in error_sinks

    def test_explicit_error_sinks_override_registered_sinks(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """AC-5: explicitly passed error_sinks take precedence over registry-resolved ones."""
        config_path = tmp_path / "streaming.yaml"
        config_path.write_text(
            """
kafka:
  consumer:
    brokers: ["localhost:9092"]
    group_id: "test"
    delivery: at_most_once
    topics: ["orders.in"]
  producer:
    brokers: ["localhost:9092"]
    client_id: "test-producer"
    topic: "orders.out"
streaming:
  sinks:
    my_sink:
      type: registry_test_sink
      some_field: value
""".strip()
        )

        runner = StreamingRunner.from_yaml(bytewax_stream_flow, str(config_path))
        runner.register_sink(_RegistryFakeSink)

        explicit_sink = object()
        received_kwargs: dict[str, object] = {}

        def _fake_prepare_run(
            plan: object,
            *,
            observability_runtime: object = None,
            runtime: object = None,
            **kwargs: object,
        ) -> object:
            received_kwargs.update(kwargs)
            return SimpleNamespace(dataflow=Dataflow("test"), shutdown=lambda: None)

        monkeypatch.setattr("loom.streaming.bytewax.runner._prepare_run", _fake_prepare_run)
        monkeypatch.setattr("loom.streaming.bytewax.runner.cli_main", lambda *_a, **_kw: None)

        explicit_map = {ErrorKind.TASK: explicit_sink}
        runner.run(config_path=str(config_path), error_sinks=explicit_map)

        passed_sinks = received_kwargs.get("error_sinks")
        assert passed_sinks is explicit_map

    def test_prepare_run_direct_without_run_ignores_registry(
        self,
        bytewax_stream_flow: StreamFlow[Order, Result],
        bytewax_runtime_config_dict: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """AC-4 + AC-6: calling prepare_run() directly (not via run()) skips registry resolution."""
        runner = StreamingRunner.from_dict(bytewax_stream_flow, bytewax_runtime_config_dict)
        runner.register_sink(_RegistryFakeSink)

        received_kwargs: dict[str, object] = {}

        def _fake_prepare_run(
            plan: object,
            *,
            observability_runtime: object = None,
            runtime: object = None,
            **kwargs: object,
        ) -> object:
            received_kwargs.update(kwargs)
            return SimpleNamespace(dataflow=Dataflow("test"), shutdown=lambda: None)

        monkeypatch.setattr("loom.streaming.bytewax.runner._prepare_run", _fake_prepare_run)

        # Call prepare_run without going through run()
        runner.prepare_run()

        # error_sinks should be None or absent — registry is not auto-resolved here
        assert received_kwargs.get("error_sinks") is None


class TestKeyedMultiprocessGuard:
    def test_at_least_once_with_collect_batch_on_cluster_fails_fast(self) -> None:
        from loom.streaming.bytewax import RuntimeConfigurationError
        from loom.streaming.bytewax import runner as runner_mod
        from loom.streaming.compiler import StreamingErrorCode
        from loom.streaming.nodes._shape import CollectBatch
        from tests.unit.streaming.bytewax.cases import build_compiled_plan

        plan = build_compiled_plan(CollectBatch(max_records=10, timeout_ms=100))
        runtime = runner_mod.BytewaxRuntimeConfig(addresses=("host-a:2101", "host-b:2101"))
        tracker = object()

        # a runtime rejection, not a compilation failure: the plan compiled fine
        with pytest.raises(RuntimeConfigurationError) as exc_info:
            runner_mod._guard_keyed_multiprocess(plan, runtime, tracker)

        codes = {issue.code for issue in exc_info.value.issues}
        assert StreamingErrorCode.DELIVERY_KEYED_MULTIPROCESS in codes

    def test_single_process_or_at_most_once_passes(self) -> None:
        from loom.streaming.bytewax import runner as runner_mod
        from loom.streaming.nodes._shape import CollectBatch
        from tests.unit.streaming.bytewax.cases import build_compiled_plan

        plan = build_compiled_plan(CollectBatch(max_records=10, timeout_ms=100))
        cluster = runner_mod.BytewaxRuntimeConfig(addresses=("host-a:2101",))
        single = runner_mod.BytewaxRuntimeConfig()

        runner_mod._guard_keyed_multiprocess(plan, single, object())  # threads: supported
        runner_mod._guard_keyed_multiprocess(plan, cluster, object())  # 1-process cluster
        runner_mod._guard_keyed_multiprocess(plan, cluster, None)  # at_most_once: no tracker
        runner_mod._guard_keyed_multiprocess(plan, None, object())  # no runtime declared
