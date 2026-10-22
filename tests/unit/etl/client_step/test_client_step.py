"""Tests for ClientStep, IntoClient, ClientSpec, and ClickHouseClientExecutor."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.etl import ETLParams, ETLPipeline, ETLProcess
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.target._client import ClientSpec, IntoClient
from loom.etl.executor import ETLExecutor
from loom.etl.io.targets._clickhouse import ClickHouseClientExecutor
from loom.etl.pipeline._step_client import ClientStep
from loom.etl.runtime.contracts import ClientCommandExecutor
from loom.etl.testing import StubSourceReader, StubTargetWriter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class SimpleParams(ETLParams):  # type: ignore[misc]
    value: int


class _ConcreteClientStep(ClientStep[SimpleParams]):
    def execute(self, params: SimpleParams, *, client: Any) -> None:  # type: ignore[override]
        client.command(f"SELECT {params.value}")


class _ProcessWithClientStep(ETLProcess[SimpleParams]):
    steps = [_ConcreteClientStep]


class _PipelineWithClientStep(ETLPipeline[SimpleParams]):
    processes = [_ProcessWithClientStep]


def _make_executor(client_executor: ClientCommandExecutor | None = None) -> ETLExecutor:
    return ETLExecutor(
        StubSourceReader({}),
        StubTargetWriter(),
        client_executor=client_executor,
    )


# ---------------------------------------------------------------------------
# IntoClient / ClientSpec
# ---------------------------------------------------------------------------


class TestIntoClientSpec:
    def test_into_client_produces_client_spec(self) -> None:
        spec = IntoClient()._to_spec()
        assert isinstance(spec, ClientSpec)

    def test_client_spec_kind(self) -> None:
        assert ClientSpec().kind == "client"

    def test_client_spec_is_frozen(self) -> None:
        from dataclasses import FrozenInstanceError

        spec = ClientSpec()
        with pytest.raises(FrozenInstanceError):
            spec.kind = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# ClientStep class-level validation
# ---------------------------------------------------------------------------


class TestClientStepDefinition:
    def test_target_defaults_to_into_client(self) -> None:
        assert isinstance(_ConcreteClientStep.target, IntoClient)

    def test_target_override_rejected_at_definition_time(self) -> None:
        from loom.etl import IntoTable

        table_target = IntoTable("some.table").replace()

        with pytest.raises(TypeError, match="target.*must remain IntoClient"):

            class _BadStep(ClientStep[SimpleParams]):
                target = table_target  # type: ignore[assignment]

                def execute(self, params: SimpleParams, *, client: Any) -> None:  # type: ignore[override]
                    pass

    def test_target_override_with_into_client_is_allowed(self) -> None:
        class _ExplicitClientStep(ClientStep[SimpleParams]):
            target = IntoClient()

            def execute(self, params: SimpleParams, *, client: Any) -> None:  # type: ignore[override]
                pass

    def test_not_implemented_when_execute_not_overridden(self) -> None:
        class _AbstractClientStep(ClientStep[SimpleParams]):
            pass

        step = _AbstractClientStep()
        params = SimpleParams(value=1)
        with pytest.raises(NotImplementedError):
            step.execute(params)


# ---------------------------------------------------------------------------
# Compiler integration
# ---------------------------------------------------------------------------


class TestClientStepCompile:
    def test_compile_step_succeeds(self) -> None:
        plan = ETLCompiler().compile_step(_ConcreteClientStep)
        assert isinstance(plan.target_binding.spec, ClientSpec)
        assert plan.source_bindings == ()

    def test_compile_step_params_type_resolved(self) -> None:
        plan = ETLCompiler().compile_step(_ConcreteClientStep)
        assert plan.params_type is SimpleParams

    def test_compile_pipeline_with_client_step_succeeds(self) -> None:
        plan = ETLCompiler().compile(_PipelineWithClientStep)
        assert plan is not None

    def test_compile_step_streaming_flag_is_false(self) -> None:
        plan = ETLCompiler().compile_step(_ConcreteClientStep)
        assert plan.streaming is False


# ---------------------------------------------------------------------------
# Executor routing
# ---------------------------------------------------------------------------


class TestClientStepExecution:
    def test_execute_injects_client_from_executor(self) -> None:
        mock_client = MagicMock()
        received: list[Any] = []

        class _RecordingStep(ClientStep[SimpleParams]):
            def execute(self, params: SimpleParams, *, client: Any) -> None:  # type: ignore[override]
                received.append(client)

        class _MockExecutor:
            def command(self, fn: Callable[[Any], None]) -> None:
                fn(mock_client)

        executor = ETLExecutor(
            StubSourceReader({}),
            StubTargetWriter(),
            client_executor=_MockExecutor(),  # type: ignore[arg-type]
        )
        plan = ETLCompiler().compile_step(_RecordingStep)
        executor.run_step(plan, SimpleParams(value=42))

        (client,) = received
        assert client is mock_client

    def test_params_forwarded_to_execute(self) -> None:
        received_params: list[Any] = []

        class _ParamsCapture(ClientStep[SimpleParams]):
            def execute(self, params: SimpleParams, *, client: Any) -> None:  # type: ignore[override]
                received_params.append(params)

        class _NullClientExecutor:
            def command(self, fn: Callable[[Any], None]) -> None:
                fn(object())

        executor = ETLExecutor(
            StubSourceReader({}),
            StubTargetWriter(),
            client_executor=_NullClientExecutor(),  # type: ignore[arg-type]
        )
        plan = ETLCompiler().compile_step(_ParamsCapture)
        executor.run_step(plan, SimpleParams(value=7))

        (param,) = received_params
        assert param.value == 7

    def test_no_sources_read_for_client_step(self) -> None:
        reader = StubSourceReader({})
        read_calls: list[Any] = []
        original_read = reader.read

        def _spy_read(spec: Any, params: Any, /) -> Any:
            read_calls.append(spec)
            return original_read(spec, params)

        reader.read = _spy_read  # type: ignore[method-assign]

        mock_client = MagicMock()

        class _MockClientExecutor:
            def command(self, fn: Callable[[Any], None]) -> None:
                fn(mock_client)

        executor = ETLExecutor(
            reader,
            StubTargetWriter(),
            client_executor=_MockClientExecutor(),  # type: ignore[arg-type]
        )
        plan = ETLCompiler().compile_step(_ConcreteClientStep)
        executor.run_step(plan, SimpleParams(value=1))

        assert not read_calls, "reader.read should not be called for ClientStep"

    def test_no_write_called_for_client_step(self) -> None:
        writer = StubTargetWriter()
        write_calls: list[Any] = []
        original_write = writer.write

        def _spy_write(frame: Any, spec: Any, params: Any, /, **kw: Any) -> None:
            write_calls.append(spec)
            return original_write(frame, spec, params, **kw)

        writer.write = _spy_write  # type: ignore[method-assign]
        mock_client = MagicMock()

        class _MockClientExecutor:
            def command(self, fn: Callable[[Any], None]) -> None:
                fn(mock_client)

        executor = ETLExecutor(
            StubSourceReader({}),
            writer,
            client_executor=_MockClientExecutor(),  # type: ignore[arg-type]
        )
        plan = ETLCompiler().compile_step(_ConcreteClientStep)
        executor.run_step(plan, SimpleParams(value=1))

        assert not write_calls, "writer.write should not be called for ClientStep"

    def test_missing_client_executor_raises_clear_error(self) -> None:
        executor = _make_executor(client_executor=None)
        plan = ETLCompiler().compile_step(_ConcreteClientStep)

        params = SimpleParams(value=1)
        with pytest.raises(TypeError, match="ClientCommandExecutor"):
            executor.run_step(plan, params)

    def test_from_config_runner_runs_client_step_without_missing_executor_error(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A runner built via from_config with a clickhouse url executes a
        ClientStep without raising the missing-executor TypeError (offline).
        """
        from loom.core.connections import ClickHouseConnection
        from loom.etl.runner import ETLRunner
        from loom.etl.storage._config import (
            StorageConfig,
            StorageDefaults,
            TablePathConfig,
        )

        fake_client = MagicMock()
        monkeypatch.setattr("clickhouse_connect.get_client", MagicMock(return_value=fake_client))

        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri=str(tmp_path))),
            clickhouse=ClickHouseConnection(url="clickhouse://user:pass@host:8123/db"),
        )
        runner = ETLRunner.from_config(config)

        runner.run(_PipelineWithClientStep, SimpleParams(value=99))

        fake_client.command.assert_called_once_with("SELECT 99")


# ---------------------------------------------------------------------------
# ClickHouseClientExecutor
# ---------------------------------------------------------------------------


class TestClickHouseClientExecutor:
    def test_command_calls_fn_with_client(self) -> None:
        mock_client = MagicMock()
        executor = ClickHouseClientExecutor(client=mock_client)

        received: list[Any] = []
        executor.command(lambda c: received.append(c))

        assert received == [mock_client]

    def test_command_passes_client_to_step_execute(self) -> None:
        mock_client = MagicMock()
        executor = ClickHouseClientExecutor(client=mock_client)
        executed_commands: list[str] = []

        def _fn(client: Any) -> None:
            client.command("OPTIMIZE TABLE orders FINAL")
            executed_commands.append("optimize")

        executor.command(_fn)

        mock_client.command.assert_called_once_with("OPTIMIZE TABLE orders FINAL")
        assert executed_commands == ["optimize"]

    def test_no_url_and_no_client_raises_on_command(self) -> None:
        executor = ClickHouseClientExecutor()
        with pytest.raises(ValueError, match="url.*client"):
            executor.command(lambda _: None)

    def test_implements_client_command_executor_protocol(self) -> None:
        mock_client = MagicMock()
        executor = ClickHouseClientExecutor(client=mock_client)
        assert isinstance(executor, ClientCommandExecutor)


# ---------------------------------------------------------------------------
# Defensive write guards
# ---------------------------------------------------------------------------


class TestClientSpecWriteGuards:
    def test_write_policy_guard_is_in_source(self) -> None:
        """Verify the ClientSpec guard is present in _write_policy.py source code."""
        import inspect

        from loom.etl.backends import _write_policy

        src = inspect.getsource(_write_policy)
        assert "ClientSpec" in src, "_write_policy must import and guard against ClientSpec"
        assert "does not support client targets" in src

    def test_clickhouse_writer_rejects_client_spec(self) -> None:
        from loom.etl.io.targets._clickhouse import ClickHouseTargetWriter

        writer = ClickHouseTargetWriter.__new__(ClickHouseTargetWriter)
        writer._client = MagicMock()  # type: ignore[attr-defined]

        spec = ClientSpec()
        params = SimpleParams(value=1)
        with pytest.raises(TypeError, match="ClientSpec"):
            writer.write(object(), spec, params)
