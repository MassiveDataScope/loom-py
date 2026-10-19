"""``bootstrap_worker`` resolves the environment like ``create_app`` does.

A worker loads ``base.yaml`` plus the overlay of its environment from a config
directory, runs the deploy checks in production and staging except the REST
ones (a worker serves no HTTP), and registers the environment in its container.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
import yaml

import loom.rest.checks as rest_checks
from loom.celery.bootstrap import bootstrap_worker
from loom.core.config.errors import ConfigError
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
from loom.core.job.job import Job
from loom.core.logger.checks import CONSOLE_RENDERER_CHECK

_CELERY = {
    "broker_url": "memory://",
    "result_backend": "cache+memory://",
    "task_always_eager": True,
}


class _SyncJob(Job[int]):
    def execute(self, value: int = 0) -> int:
        return value * 2


def _write(path: Path, document: dict[str, object]) -> str:
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return str(path)


def test_the_overlay_of_the_environment_overrides_the_base(tmp_path: Path) -> None:
    _write(tmp_path / "base.yaml", {"celery": _CELERY})
    _write(tmp_path / "staging.yaml", {"celery": {"broker_url": "memory://staging"}})

    result = bootstrap_worker(config_dir=str(tmp_path), environment="staging", jobs=[_SyncJob])

    assert result.celery_app.conf.broker_url == "memory://staging"


def test_the_environment_is_registered_in_the_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _write(tmp_path / "worker.yaml", {"celery": _CELERY})
    monkeypatch.setenv(ENVIRONMENT_VARIABLE, "development")

    result = bootstrap_worker(config, jobs=[_SyncJob])

    assert result.container.resolve(Environment) is Environment.DEVELOPMENT


def test_a_deployed_worker_runs_the_deploy_checks_but_not_the_rest_ones(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config = _write(
        tmp_path / "worker.yaml",
        {
            "app": {"name": "worker"},
            "celery": _CELERY,
            "observability": {
                "log": {"config": {"name": "loom.tests.worker", "renderer": "console"}}
            },
        },
    )

    with caplog.at_level(logging.WARNING, logger="loom.core.checks"):
        bootstrap_worker(config, environment="production", jobs=[_SyncJob])

    messages = " ".join(r.getMessage() for r in caplog.records)
    assert CONSOLE_RENDERER_CHECK in messages
    assert rest_checks.AUTHENTICATION_CHECK not in messages
    assert rest_checks.SCHEMA_CHECK not in messages


def test_files_and_a_directory_together_are_refused(tmp_path: Path) -> None:
    config = _write(tmp_path / "base.yaml", {"celery": _CELERY})

    with pytest.raises(ConfigError, match="config_dir"):
        bootstrap_worker(config, config_dir=str(tmp_path), jobs=[_SyncJob])


def test_a_removed_logger_key_stops_the_worker(tmp_path: Path) -> None:
    observability = {"log": {"config": {"environment": "dev"}}}
    config = _write(tmp_path / "worker.yaml", {"celery": _CELERY, "observability": observability})

    with pytest.raises(ConfigError, match="environment"):
        bootstrap_worker(config, environment="development", jobs=[_SyncJob])
