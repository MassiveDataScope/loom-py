"""Tests for ``resolvers=`` and the built-in resolver defaults in ``bootstrap_worker``."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.celery.bootstrap import bootstrap_worker
from loom.core.config import ConfigError
from loom.core.job.job import Job
from tests.helpers.extras import fake_aws_island, without_boto3
from tests.unit._resolver_stubs import MappingResolver

pytestmark = pytest.mark.usefixtures("clear_builtin_resolvers")


class _SyncJob(Job[int]):
    def execute(self, value: int = 0) -> int:
        return value * 2


def _write_worker_yaml(tmp_path: Path, broker_url: str) -> str:
    path = tmp_path / "worker.yaml"
    path.write_text(
        "app:\n"
        "  name: worker\n"
        "celery:\n"
        f"  broker_url: {broker_url}\n"
        "  result_backend: cache+memory://\n"
        "  task_always_eager: true\n"
        "stores:\n"
        "  default:\n"
        "    backend: sqlalchemy\n"
        "    url: sqlite+aiosqlite:///test.db\n",
        encoding="utf-8",
    )
    return str(path)


def _without_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    without_boto3(monkeypatch)


class TestBootstrapResolvers:
    def test_user_resolver_reaches_the_celery_app(self, tmp_path: Path) -> None:
        config_path = _write_worker_yaml(tmp_path, "${stub:broker}")

        result = bootstrap_worker(
            config_path,
            jobs=[_SyncJob],
            resolvers=[MappingResolver("stub", {"broker": "memory://"})],
        )

        assert result.celery_app.conf.broker_url == "memory://"

    def test_secrets_placeholder_without_boto3_names_the_extra(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _without_boto3(monkeypatch)
        config_path = _write_worker_yaml(tmp_path, "${secrets:/worker/broker}")

        with pytest.raises(ConfigError, match=r"loom-kernel\[aws\]"):
            bootstrap_worker(config_path, jobs=[_SyncJob])

    def test_no_aws_client_without_placeholders(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        island = fake_aws_island(monkeypatch)
        config_path = _write_worker_yaml(tmp_path, "memory://")

        result = bootstrap_worker(config_path, jobs=[_SyncJob])

        assert result.celery_app.conf.broker_url == "memory://"
        assert island.sessions == []

    def test_user_secrets_resolver_wins_over_the_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _without_boto3(monkeypatch)
        config_path = _write_worker_yaml(tmp_path, "${secrets:/worker/broker}")

        result = bootstrap_worker(
            config_path,
            jobs=[_SyncJob],
            resolvers=[MappingResolver("secrets", {"/worker/broker": "memory://"})],
        )

        assert result.celery_app.conf.broker_url == "memory://"


class TestBootstrapWorkerDefaultConfig:
    def test_without_a_path_the_worker_loads_the_default_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        directory = tmp_path / "config"
        directory.mkdir()
        written = Path(_write_worker_yaml(tmp_path, "memory://"))
        (directory / "base.yaml").write_bytes(written.read_bytes())
        monkeypatch.delenv("LOOM_CONFIG_DIR", raising=False)
        monkeypatch.chdir(tmp_path)

        result = bootstrap_worker(jobs=[_SyncJob])

        assert result.celery_app.conf.broker_url == "memory://"
