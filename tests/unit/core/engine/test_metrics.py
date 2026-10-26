from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from loom.core.command import Command
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import NotFound, RuleViolation, RuleViolations
from loom.core.use_case.markers import Input, LoadById
from loom.core.use_case.use_case import UseCase

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class Cmd(Command, frozen=True):
    value: str


class Entity:
    pass


class _RecordingAdapter:
    """In-memory MetricsAdapter for test assertions."""

    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    def on_event(self, event: RuntimeEvent) -> None:
        self.events.append(event)

    def kinds(self) -> list[EventKind]:
        return [e.kind for e in self.events]

    def by_kind(self, kind: EventKind) -> list[RuntimeEvent]:
        return [e for e in self.events if e.kind == kind]


class _SimpleUseCase(UseCase[Any, str]):
    async def execute(self, cmd: Cmd = Input()) -> str:
        return cmd.value


class _FailingUseCase(UseCase[Any, str]):
    async def execute(self, cmd: Cmd = Input()) -> str:
        raise RuntimeError("boom")


def _always_bad(
    command: Command, fields_set: frozenset[str], context: dict[str, object] | None = None
) -> None:
    raise RuleViolation("value", "bad")


class _RuleFailUseCase(UseCase[Any, str]):
    rules = [_always_bad]

    async def execute(self, cmd: Cmd = Input()) -> str:
        return cmd.value


class _LoadUseCase(UseCase[Any, str]):
    async def execute(
        self,
        eid: int,
        entity: Entity = LoadById(Entity, by="eid"),
    ) -> str:
        return "ok"


def _make_compiler(adapter: _RecordingAdapter | None = None) -> UseCaseCompiler:
    return UseCaseCompiler(metrics=adapter)


def _make_executor(
    compiler: UseCaseCompiler,
    adapter: _RecordingAdapter | None = None,
) -> RuntimeExecutor:
    return RuntimeExecutor(compiler, metrics=adapter)


# ---------------------------------------------------------------------------
# MetricsAdapter protocol
# ---------------------------------------------------------------------------


class TestMetricsAdapterProtocol:
    def test_recording_adapter_satisfies_protocol(self) -> None:
        adapter = _RecordingAdapter()
        event = RuntimeEvent(kind=EventKind.EXEC_DONE, use_case_name="X")
        # duck-typing — no TypeError expected
        adapter.on_event(event)
        assert len(adapter.events) == 1

    def test_none_metrics_does_not_crash_executor(self) -> None:
        compiler = _make_compiler()
        executor = _make_executor(compiler, adapter=None)
        assert executor._metrics is None


# ---------------------------------------------------------------------------
# Compiler events
# ---------------------------------------------------------------------------


class TestCompilerEvents:
    def test_emits_compile_start(self) -> None:
        adapter = _RecordingAdapter()
        _make_compiler(adapter).compile(_SimpleUseCase)
        assert EventKind.COMPILE_START in adapter.kinds()

    def test_emits_compile_done(self) -> None:
        adapter = _RecordingAdapter()
        _make_compiler(adapter).compile(_SimpleUseCase)
        assert EventKind.COMPILE_DONE in adapter.kinds()

    def test_compile_events_carry_use_case_name(self) -> None:
        adapter = _RecordingAdapter()
        _make_compiler(adapter).compile(_SimpleUseCase)
        done = adapter.by_kind(EventKind.COMPILE_DONE)[0]
        assert done.use_case_name == "_SimpleUseCase"

    def test_no_events_on_cache_hit(self) -> None:
        adapter = _RecordingAdapter()
        compiler = _make_compiler(adapter)
        compiler.compile(_SimpleUseCase)
        count = len(adapter.events)
        compiler.compile(_SimpleUseCase)  # cache hit
        assert len(adapter.events) == count


# ---------------------------------------------------------------------------
# Executor events — success path
# ---------------------------------------------------------------------------


class TestExecutorEventsSuccess:
    async def test_emits_exec_start(self) -> None:
        adapter = _RecordingAdapter()
        compiler = _make_compiler()
        executor = _make_executor(compiler, adapter)
        await executor.execute(_SimpleUseCase(), payload={"value": "hi"})
        assert EventKind.EXEC_START in adapter.kinds()

    async def test_emits_exec_done(self) -> None:
        adapter = _RecordingAdapter()
        compiler = _make_compiler()
        executor = _make_executor(compiler, adapter)
        await executor.execute(_SimpleUseCase(), payload={"value": "hi"})
        assert EventKind.EXEC_DONE in adapter.kinds()

    async def test_exec_done_has_success_status(self) -> None:
        adapter = _RecordingAdapter()
        compiler = _make_compiler()
        executor = _make_executor(compiler, adapter)
        await executor.execute(_SimpleUseCase(), payload={"value": "hi"})
        done = adapter.by_kind(EventKind.EXEC_DONE)[0]
        assert done.status == "success"

    async def test_exec_done_has_duration_ms(self) -> None:
        adapter = _RecordingAdapter()
        compiler = _make_compiler()
        executor = _make_executor(compiler, adapter)
        await executor.execute(_SimpleUseCase(), payload={"value": "hi"})
        done = adapter.by_kind(EventKind.EXEC_DONE)[0]
        assert done.duration_ms is not None
        assert done.duration_ms >= 0

    async def test_exec_done_carries_use_case_name(self) -> None:
        adapter = _RecordingAdapter()
        compiler = _make_compiler()
        executor = _make_executor(compiler, adapter)
        await executor.execute(_SimpleUseCase(), payload={"value": "hi"})
        done = adapter.by_kind(EventKind.EXEC_DONE)[0]
        assert done.use_case_name == "_SimpleUseCase"

    async def test_exec_done_carries_pipeline_ms_and_no_commit_ms_without_uow(self) -> None:
        adapter = _RecordingAdapter()
        executor = _make_executor(_make_compiler(), adapter)
        await executor.execute(_SimpleUseCase(), payload={"value": "hi"})
        done = adapter.by_kind(EventKind.EXEC_DONE)[0]
        assert done.pipeline_ms is not None
        assert done.commit_ms is None
        assert done.duration_ms is not None
        assert done.duration_ms >= done.pipeline_ms

    async def test_no_exec_error_on_success(self) -> None:
        adapter = _RecordingAdapter()
        compiler = _make_compiler()
        executor = _make_executor(compiler, adapter)
        await executor.execute(_SimpleUseCase(), payload={"value": "hi"})
        assert EventKind.EXEC_ERROR not in adapter.kinds()


# ---------------------------------------------------------------------------
# Executor events — failure path (generic exception)
# ---------------------------------------------------------------------------


class TestExecutorEventsFailure:
    async def test_emits_exec_error_on_exception(self) -> None:
        adapter = _RecordingAdapter()
        compiler = _make_compiler()
        executor = _make_executor(compiler, adapter)
        use_case = _FailingUseCase()
        with pytest.raises(RuntimeError):
            await executor.execute(use_case, payload={"value": "x"})
        assert EventKind.EXEC_ERROR in adapter.kinds()

    async def test_exec_error_has_failure_status(self) -> None:
        adapter = _RecordingAdapter()
        compiler = _make_compiler()
        executor = _make_executor(compiler, adapter)
        use_case = _FailingUseCase()
        with pytest.raises(RuntimeError):
            await executor.execute(use_case, payload={"value": "x"})
        err = adapter.by_kind(EventKind.EXEC_ERROR)[0]
        assert err.status == "failure"

    async def test_exec_error_carries_exception(self) -> None:
        adapter = _RecordingAdapter()
        compiler = _make_compiler()
        executor = _make_executor(compiler, adapter)
        use_case = _FailingUseCase()
        with pytest.raises(RuntimeError):
            await executor.execute(use_case, payload={"value": "x"})
        err = adapter.by_kind(EventKind.EXEC_ERROR)[0]
        assert isinstance(err.error, RuntimeError)

    async def test_exec_error_has_duration_ms(self) -> None:
        adapter = _RecordingAdapter()
        compiler = _make_compiler()
        executor = _make_executor(compiler, adapter)
        use_case = _FailingUseCase()
        with pytest.raises(RuntimeError):
            await executor.execute(use_case, payload={"value": "x"})
        err = adapter.by_kind(EventKind.EXEC_ERROR)[0]
        assert err.duration_ms is not None

    async def test_exec_error_names_the_failed_phase(self) -> None:
        adapter = _RecordingAdapter()
        executor = _make_executor(_make_compiler(), adapter)
        use_case = _FailingUseCase()
        with pytest.raises(RuntimeError):
            await executor.execute(use_case, payload={"value": "x"})
        err = adapter.by_kind(EventKind.EXEC_ERROR)[0]
        assert err.error_kind == "business"
        assert err.pipeline_ms is not None

    async def test_exception_is_reraised(self) -> None:
        executor = _make_executor(_make_compiler())
        use_case = _FailingUseCase()
        with pytest.raises(RuntimeError, match="boom"):
            await executor.execute(use_case, payload={"value": "x"})


# ---------------------------------------------------------------------------
# Executor events — rule_failure path
# ---------------------------------------------------------------------------


class TestExecutorEventsRuleFailure:
    async def test_rule_failure_emits_exec_error(self) -> None:
        adapter = _RecordingAdapter()
        executor = _make_executor(_make_compiler(), adapter)
        use_case = _RuleFailUseCase()
        with pytest.raises(RuleViolations):
            await executor.execute(use_case, payload={"value": "x"})
        assert adapter.kinds() == [EventKind.EXEC_START, EventKind.EXEC_ERROR]

    async def test_rule_failure_status_is_rule_failure(self) -> None:
        adapter = _RecordingAdapter()
        executor = _make_executor(_make_compiler(), adapter)
        use_case = _RuleFailUseCase()
        with pytest.raises(RuleViolations):
            await executor.execute(use_case, payload={"value": "x"})
        err = adapter.by_kind(EventKind.EXEC_ERROR)[0]
        assert err.status == "rule_failure"

    async def test_rule_failure_carries_rule_violations(self) -> None:
        adapter = _RecordingAdapter()
        executor = _make_executor(_make_compiler(), adapter)
        use_case = _RuleFailUseCase()
        with pytest.raises(RuleViolations):
            await executor.execute(use_case, payload={"value": "x"})
        err = adapter.by_kind(EventKind.EXEC_ERROR)[0]
        assert isinstance(err.error, RuleViolations)


# ---------------------------------------------------------------------------
# Executor events — NotFound path
# ---------------------------------------------------------------------------


class TestExecutorEventsNotFound:
    async def test_not_found_emits_exec_error_with_failure_status(self) -> None:
        adapter = _RecordingAdapter()
        executor = _make_executor(_make_compiler(), adapter)
        repo = AsyncMock()
        repo.get_by_id = AsyncMock(return_value=None)
        use_case = _LoadUseCase()
        with pytest.raises(NotFound):
            await executor.execute(
                use_case,
                params={"eid": 1},
                dependencies={Entity: repo},
            )
        err = adapter.by_kind(EventKind.EXEC_ERROR)[0]
        assert err.status == "failure"
        assert isinstance(err.error, NotFound)


# ---------------------------------------------------------------------------
# Structured log fields
# ---------------------------------------------------------------------------


class _RecordingLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def bind(self, **fields: Any) -> _RecordingLogger:
        return self

    def debug(self, event: str, **fields: Any) -> None:
        self.calls.append((event, fields))

    def info(self, event: str, **fields: Any) -> None:
        self.calls.append((event, fields))

    def warning(self, event: str, **fields: Any) -> None:
        self.calls.append((event, fields))

    def error(self, event: str, **fields: Any) -> None:
        self.calls.append((event, fields))

    def exception(self, event: str, **fields: Any) -> None:
        self.calls.append((event, fields))

    def fields_for(self, substr: str) -> dict[str, Any]:
        for msg, fields in self.calls:
            if substr in msg:
                return fields
        return {}


class TestStructuredLogs:
    async def test_exec_start_has_usecase_field(self) -> None:
        log = _RecordingLogger()
        compiler = UseCaseCompiler(logger=log)
        executor = RuntimeExecutor(compiler, logger=log)
        await executor.execute(_SimpleUseCase(), payload={"value": "x"})
        fields = log.fields_for("[EXEC]")
        assert fields.get("usecase") == "_SimpleUseCase"

    async def test_done_has_duration_ms_and_status(self) -> None:
        log = _RecordingLogger()
        compiler = UseCaseCompiler(logger=log)
        executor = RuntimeExecutor(compiler, logger=log)
        await executor.execute(_SimpleUseCase(), payload={"value": "x"})
        fields = log.fields_for("[DONE]")
        assert "duration_ms" in fields
        assert fields.get("status") == "success"

    async def test_fail_log_has_status_failure(self) -> None:
        log = _RecordingLogger()
        compiler = UseCaseCompiler(logger=log)
        executor = RuntimeExecutor(compiler, logger=log)
        use_case = _FailingUseCase()
        with pytest.raises(RuntimeError):
            await executor.execute(use_case, payload={"value": "x"})
        fields = log.fields_for("[FAIL]")
        assert fields.get("status") == "failure"

    async def test_compile_log_has_usecase_field(self) -> None:
        log = _RecordingLogger()
        UseCaseCompiler(logger=log).compile(_SimpleUseCase)
        fields = log.fields_for("[BOOT] Compiling UseCase")
        assert fields.get("usecase") == "_SimpleUseCase"
