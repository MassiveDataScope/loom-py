"""``create_app`` builds the worker's Celery application from a configuration file."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.celery.auto import create_app
from loom.core.engine.events import EventKind
from loom.core.job.envelope import wire_job_name
from loom.core.job.job import Job
from tests.helpers.jobs import RecordingExecutionObserver

WORKER_CONFIG = """\
app:
  name: worker
celery:
  broker_url: memory://
  result_backend: cache+memory://
  task_always_eager: true
stores:
  default:
    backend: sqlalchemy
    url: sqlite+aiosqlite:///test.db
"""


class AutoJob(Job[int]):
    def execute(self) -> int:
        return 1


@pytest.fixture
def config_path(tmp_path: Path) -> str:
    path = tmp_path / "worker.yaml"
    path.write_text(WORKER_CONFIG, encoding="utf-8")
    return str(path)


class TestCreateApp:
    def test_the_application_serves_the_jobs_it_is_given(self, config_path: str) -> None:
        app = create_app(config_path, jobs=[AutoJob])

        assert wire_job_name(AutoJob.__qualname__) in app.tasks

    def test_the_application_runs_a_job_and_reports_it_to_the_execution_observer(
        self, config_path: str
    ) -> None:
        observer = RecordingExecutionObserver()
        app = create_app(config_path, jobs=[AutoJob], execution_observer=observer)

        result = app.tasks[wire_job_name(AutoJob.__qualname__)].apply().get()

        assert result == 1
        assert EventKind.JOB_SUCCEEDED in observer.kinds
