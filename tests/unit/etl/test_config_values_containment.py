"""A FromConfig value never leaves the step: no log, repr, plan, event or error carries it."""

from __future__ import annotations

import logging
import traceback
from datetime import date
from typing import Any, Literal

import pytest

from loom.core.config import ConfigContext
from loom.core.observability.observability import ObservabilityRuntime
from loom.etl import ETLPipeline, ETLProcess, ETLStep, FromConfig, IntoTable
from loom.etl.compiler import ETLCompilationError, ETLCompiler
from loom.etl.executor import ETLExecutor
from loom.etl.ports import ConfigValueError
from loom.etl.runner import ETLRunner
from loom.etl.testing import StubRunObserver, StubSourceReader, StubTargetWriter
from tests.unit.etl.doubles import RunDateIgnoredParams

SECRET = "s3cr3t-respondio-token-value"


class TokenStep(ETLStep[RunDateIgnoredParams]):
    api_token = FromConfig("respondio.api_token")
    target = IntoTable("raw.token").replace()

    def execute(self, params: RunDateIgnoredParams, *, api_token: str) -> Any:  # type: ignore[override]
        return {"token_length": len(api_token)}


class ModeStep(ETLStep[RunDateIgnoredParams]):
    # msgspec names the rejected value for a Literal: the case most likely to leak.
    mode = FromConfig("respondio.api_token", Literal["read", "write"])
    target = IntoTable("raw.mode").replace()

    def execute(self, params: RunDateIgnoredParams, *, mode: str) -> Any:  # type: ignore[override]
        return mode


class _Proc(ETLProcess[RunDateIgnoredParams]):
    steps = [TokenStep]


class _Pipeline(ETLPipeline[RunDateIgnoredParams]):
    processes = [_Proc]


_PARAMS = RunDateIgnoredParams(run_date=date(2026, 9, 25))


def _context() -> ConfigContext:
    return ConfigContext.from_dict({"respondio": {"api_token": SECRET}})


def _rendered(exc: BaseException) -> str:
    return "".join(traceback.format_exception(exc))


def _chain(exc: BaseException) -> list[BaseException]:
    """Every exception reachable through ``__cause__`` and ``__context__``."""
    found: list[BaseException] = []
    pending: list[BaseException | None] = [exc]
    while pending:
        current = pending.pop()
        if current is None or any(current is seen for seen in found):
            continue
        found.append(current)
        pending.extend([current.__cause__, current.__context__])
    return found


def _frame_locals(exc: BaseException) -> list[str]:
    """Repr of every local in every frame of *exc*'s traceback."""
    rendered: list[str] = []
    tb = exc.__traceback__
    while tb is not None:
        rendered.extend(repr(value) for value in tb.tb_frame.f_locals.values())
        tb = tb.tb_next
    return rendered


def _assert_contained(exc: BaseException) -> None:
    assert SECRET not in _rendered(exc)
    for linked in _chain(exc):
        assert SECRET not in str(linked)
        assert SECRET not in repr(linked.args)
    assert all(SECRET not in value for value in _frame_locals(exc))


class TestConfigValueContainment:
    def test_successful_run_leaks_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        observer = StubRunObserver()
        writer = StubTargetWriter()
        runner = ETLRunner(
            StubSourceReader({}),
            writer,
            ObservabilityRuntime([observer]),
            config_context=_context(),
        )

        with caplog.at_level(logging.DEBUG):
            runner.run(_Pipeline, _PARAMS)

        assert writer.written[0][0] == {"token_length": len(SECRET)}
        assert SECRET not in caplog.text
        assert all(SECRET not in repr(data) for _, data in observer.events)
        assert SECRET not in repr(ETLCompiler().compile(_Pipeline))
        assert SECRET not in repr(writer.written[0][1])
        assert SECRET not in repr(TokenStep.api_token)
        assert SECRET not in repr(runner)

    def test_compile_error_does_not_carry_the_value(self) -> None:
        with pytest.raises(ETLCompilationError) as exc_info:
            ETLCompiler(config_context=_context()).compile_step(ModeStep)

        assert "does not validate" in str(exc_info.value)
        assert exc_info.value.__context__ is None
        _assert_contained(exc_info.value)

    def test_runtime_error_and_error_event_do_not_carry_the_value(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        observer = StubRunObserver()
        executor = ETLExecutor(
            StubSourceReader({}),
            StubTargetWriter(),
            ObservabilityRuntime([observer]),
            config_context=_context(),
        )

        with caplog.at_level(logging.DEBUG), pytest.raises(ConfigValueError) as exc_info:
            executor.run_step(ETLCompiler().compile_step(ModeStep), _PARAMS)

        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None
        _assert_contained(exc_info.value)
        assert SECRET not in caplog.text
        assert all(SECRET not in repr(data) for _, data in observer.events)
