"""An application's own execution observers: composed, fed by its jobs, checked by loom check."""

from __future__ import annotations

import logging
import sys
import uuid
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from loom.cli.app import app
from loom.composition import Project, Recipe
from loom.core.config import ConfigError
from loom.core.engine.events import AbandonReason, EventKind, ExecutionEvent
from loom.core.engine.execution_observer import ExecutionObserver
from loom.core.job.service import JobService
from tests.helpers.execution_observers import (
    NOT_AN_OBSERVER,
    RAISING,
    RECORDING,
    GiveUpJob,
    JobOutcomeRecorder,
    PostponeForLongJob,
)
from tests.helpers.handoffs import echoed_failures
from tests.helpers.jobs import JobsAdapter, job_project

runner = CliRunner()


def _declaring(*entries: str) -> dict[str, object]:
    return {"observability": {"execution_observers": list(entries)}}


class _OwnObserver:
    """Stands for the observer loom builds itself, such as Prometheus's."""

    def __init__(self) -> None:
        self.events: list[ExecutionEvent] = []

    def on_event(self, event: ExecutionEvent) -> None:
        self.events.append(event)

    @property
    def abandonments(self) -> list[AbandonReason | None]:
        return [event.reason for event in self.events if event.kind is EventKind.JOB_ABANDONED]


class _WithOwnObserver(JobsAdapter):
    """Composes the jobs with an observer loom itself would have built, such as Prometheus's."""

    def __init__(self, own: ExecutionObserver, *jobs: type) -> None:
        super().__init__(*jobs)
        self._own = own

    def recipe(self, project: Project) -> Recipe:
        return replace(super().recipe(project), execution_observer=self._own)


@pytest.fixture(autouse=True)
def _no_events() -> None:
    JobOutcomeRecorder.events.clear()


async def _run_give_up_job(project: Project, adapter: JobsAdapter) -> None:
    application = project.compose(adapter)
    async with application.running():
        await application.jobs.dispatch_now(GiveUpJob)
        await application.jobs.wait_until_idle()


class TestAbandonedJob:
    async def test_the_declared_observer_receives_the_abandonment_with_its_reason(
        self, tmp_path: Path
    ) -> None:
        project = job_project(tmp_path, _declaring(RECORDING), "inline")

        await _run_give_up_job(project, JobsAdapter(GiveUpJob))

        assert JobOutcomeRecorder.abandonments() == [AbandonReason.JOB_ASKED]

    async def test_a_postponed_job_lost_at_exit_reaches_the_observer_as_a_shutdown(
        self, tmp_path: Path
    ) -> None:
        project = job_project(tmp_path, _declaring(RECORDING), "inline")
        application = project.compose(JobsAdapter(PostponeForLongJob))

        async with application.running():
            await application.kernel.container.resolve(JobService).dispatch_now(PostponeForLongJob)
            assert JobOutcomeRecorder.abandonments() == []

        assert JobOutcomeRecorder.abandonments() == [AbandonReason.SHUTDOWN]

    async def test_a_raising_observer_breaks_neither_the_run_nor_the_others(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        own = _OwnObserver()
        project = job_project(tmp_path, _declaring(RAISING, RECORDING), "inline")

        with caplog.at_level(logging.WARNING):
            await _run_give_up_job(project, _WithOwnObserver(own, GiveUpJob))

        assert own.abandonments == [AbandonReason.JOB_ASKED]
        assert JobOutcomeRecorder.abandonments() == [AbandonReason.JOB_ASKED]
        assert caplog.messages.count("engine.execution_observer_failed") == len(own.events)

    async def test_a_module_under_the_code_path_resolves(self, tmp_path: Path) -> None:
        module = f"acme_observers_{uuid.uuid4().hex}"
        code = tmp_path / "src"
        code.mkdir()
        (code / f"{module}.py").write_text(
            "from tests.helpers.execution_observers import JobOutcomeRecorder\n"
            "class Recorder(JobOutcomeRecorder):\n    pass\n",
            encoding="utf-8",
        )
        (tmp_path / "base.yaml").write_text(
            yaml.safe_dump(
                {
                    "app": {"name": "acme", "job_service": "inline"},
                    **_declaring(f"{module}:Recorder"),
                }
            ),
            encoding="utf-8",
        )
        sys.modules.pop(module, None)

        project = Project.load(str(tmp_path / "base.yaml"), environment="test")
        await _run_give_up_job(project, JobsAdapter(GiveUpJob))

        assert JobOutcomeRecorder.abandonments() == [AbandonReason.JOB_ASKED]
        sys.modules.pop(module, None)
        sys.path.remove(str(code.resolve()))

    def test_an_entry_that_does_not_build_refuses_composition_naming_it(
        self, tmp_path: Path
    ) -> None:
        project = job_project(tmp_path, _declaring(NOT_AN_OBSERVER), "inline")

        with pytest.raises(ConfigError, match="not an execution observer"):
            project.compose(JobsAdapter(GiveUpJob))


class TestLoomCheck:
    def _config(self, tmp_path: Path, *entries: str) -> Path:
        path = tmp_path / "base.yaml"
        path.write_text(
            yaml.safe_dump({"app": {"name": "acme"}, **_declaring(*entries)}), encoding="utf-8"
        )
        return path

    def test_a_bad_entry_fails_the_check_naming_it(self, tmp_path: Path) -> None:
        config = self._config(tmp_path, RECORDING, "acme.missing:Observer")

        with echoed_failures() as echoed:
            result = runner.invoke(app, ["check", "--deploy", str(config), "--skip", "app.rest"])

        assert result.exit_code == 1
        assert "'acme.missing:Observer'" in echoed.messages

    def test_a_good_list_passes_the_check(self, tmp_path: Path) -> None:
        config = self._config(tmp_path, RECORDING)

        result = runner.invoke(app, ["check", "--deploy", str(config), "--skip", "app.rest"])

        assert result.exit_code == 0, result.output
