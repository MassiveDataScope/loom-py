"""Unit tests for loom.celery.bootstrap (Piece 8)."""

from __future__ import annotations

import dataclasses
import logging
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

import loom.celery._discovery as discovery
import loom.celery.adapter as adapter
import loom.celery.bootstrap as boot
from loom.celery.bootstrap import WorkerBootstrapResult
from loom.celery.config import CeleryRuntimeConfig
from loom.celery.constants import TASK_CALLBACK_ERROR_PREFIX, TASK_CALLBACK_PREFIX, TASK_JOB_PREFIX
from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
from loom.core.discovery.worker import DuplicateJobName
from loom.core.job.envelope import wire_job_name
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.registry import UseCaseRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _SyncJob(Job[int]):
    __queue__ = "default"
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self, value: int = 0) -> int:
        return value * 2


class _DiscoveredModel(BaseModel):
    __tablename__ = "discovered_model_bootstrap_test"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)


def _make_config(extra: dict[str, Any] | None = None) -> ConfigContext:
    """Build a minimal configuration for bootstrap_worker tests."""
    base = {
        "app": {"name": "worker"},
        "celery": {
            "broker_url": "memory://",
            "result_backend": "cache+memory://",
            "task_always_eager": True,
        },
        "stores": {
            "default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///test.db"},
            "settings": {"cursors": {"signing": "ephemeral"}},
        },
    }
    if extra:
        base.update(extra)
    return ConfigContext.from_dict(base)


# ---------------------------------------------------------------------------
# Duplicate job names
# ---------------------------------------------------------------------------


class TestDuplicateJobQualnames:
    def test_celery_discovery_rejects_duplicate_job_qualnames(self) -> None:
        def make_job() -> type[Job[None]]:
            class Duplicate(Job[None]):
                def execute(self) -> None:
                    return None

            return Duplicate

        jobs = [make_job(), make_job()]
        discovered = DiscoveryResult(models=(), use_cases=(), interfaces=())

        with pytest.raises(DuplicateJobName):
            discovery._resolve_compilables_and_jobs(discovered, jobs, [])


# ---------------------------------------------------------------------------
# WorkerBootstrapResult — immutability
# ---------------------------------------------------------------------------


class TestWorkerBootstrapResult:
    def test_frozen_prevents_mutation(self) -> None:
        result = WorkerBootstrapResult(
            container=MagicMock(),
            factory=MagicMock(),
            celery_app=MagicMock(),
        )
        untyped_result = cast(Any, result)
        new_container = MagicMock()
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            untyped_result.container = new_container


# ---------------------------------------------------------------------------
# bootstrap_worker — task registration
# ---------------------------------------------------------------------------


class TestBootstrapWorkerTaskRegistration:
    def _run(
        self,
        tmp_path: Any,
        extra_cfg: dict[str, Any] | None = None,
        *,
        jobs: tuple[type[Job[Any]], ...] | None = (_SyncJob,),
    ) -> WorkerBootstrapResult:
        """Write a minimal YAML config and call bootstrap_worker."""
        import yaml  # type: ignore[import-untyped]

        cfg: dict[str, Any] = {
            "celery": {
                "broker_url": "memory://",
                "result_backend": "cache+memory://",
                "task_always_eager": True,
            },
            "stores": {
                "default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///test.db"},
                "settings": {"cursors": {"signing": "ephemeral"}},
            },
        }
        if extra_cfg:
            cfg.update(extra_cfg)
        cfg["app"] = {"name": "worker", "code_path": ".", **cfg.get("app", {})}
        packages = cfg["app"].get("packages", [])
        if packages:
            package = tmp_path / packages[0]
            package.mkdir()
            (package / "__init__.py").write_text(
                "from loom.core.job.callback import JobCallback\n"
                "from loom.core.job.job import Job\n"
                "from loom.core.model import BaseModel, ColumnField\n"
                "class _SyncJob(Job[int]):\n"
                "    def execute(self, value: int = 0) -> int: return value * 2\n"
                "class _FixtureCallback(JobCallback):\n"
                "    def on_success(self, job_id, result, **context): pass\n"
                "class _DiscoveredModel(BaseModel):\n"
                "    __tablename__ = 'discovered_model_bootstrap_test'\n"
                "    id: int = ColumnField(primary_key=True, autoincrement=True)\n"
                "    name: str = ColumnField(length=120)\n"
            )

        config_path = tmp_path / "worker.yaml"
        config_path.write_text(yaml.dump(cfg))

        from loom.celery.bootstrap import bootstrap_worker

        kwargs: dict[str, Any] = {}
        if jobs is not None:
            kwargs["jobs"] = list(jobs)
        return bootstrap_worker(str(config_path), **kwargs)

    def test_task_registered_with_correct_name(self, tmp_path: Any) -> None:
        result = self._run(tmp_path)
        expected = f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}"
        assert expected in result.celery_app.tasks

    def test_celery_app_is_configured(self, tmp_path: Any) -> None:
        result = self._run(tmp_path)
        assert result.celery_app.conf.task_always_eager is True

    def test_builds_async_runtime_from_yaml(self, tmp_path: Any) -> None:
        import yaml

        runtime_cfg: dict[str, object] = {
            "backend": "trio",
            "use_uvloop": True,
            "shutdown_timeout_ms": 77,
        }
        cfg: dict[str, Any] = {
            "app": {"name": "worker"},
            "celery": {
                "broker_url": "memory://",
                "result_backend": "cache+memory://",
                "task_always_eager": True,
                "runtime": runtime_cfg,
            },
            "stores": {"default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///test.db"}},
        }
        config_path = tmp_path / "worker_with_runtime.yaml"
        config_path.write_text(yaml.dump(cfg))

        with patch.object(
            adapter, "_build_async_runtime", wraps=adapter._build_async_runtime
        ) as spy:
            boot.bootstrap_worker(str(config_path), jobs=[_SyncJob])

        call = spy.call_args
        assert call is not None
        runtime = call.args[0]
        assert isinstance(runtime, CeleryRuntimeConfig)
        assert runtime.backend == "trio"
        assert runtime.use_uvloop is True
        assert runtime.shutdown_timeout_ms == 77

    def test_returns_valid_result_type(self, tmp_path: Any) -> None:
        result = self._run(tmp_path)
        assert isinstance(result, WorkerBootstrapResult)

    def test_registers_each_task_with_the_applications_job_config(self, tmp_path: Any) -> None:
        """The task's time limit comes from ``jobs:``; the ClassVars stay as declared."""
        result = self._run(tmp_path, extra_cfg={"jobs": {"_SyncJob": {"timeout": 9}}})

        task = result.celery_app.tasks[wire_job_name(_SyncJob.__qualname__)]
        assert task.soft_time_limit == 9
        assert _SyncJob.__timeout__ is None

    def test_pure_job_bootstrap_without_database_section(self, tmp_path: Any) -> None:
        """Database config is optional for jobs that do not access repositories."""
        import yaml

        cfg: dict[str, Any] = {
            "app": {"name": "worker"},
            "celery": {
                "broker_url": "memory://",
                "result_backend": "cache+memory://",
                "task_always_eager": True,
            },
        }
        config_path = tmp_path / "worker_no_db.yaml"
        config_path.write_text(yaml.dump(cfg))

        with patch.object(adapter, "_connect_lifecycle_signals") as mock_signals:
            result = boot.bootstrap_worker(str(config_path), jobs=[_SyncJob])

        assert isinstance(result, WorkerBootstrapResult)
        mock_signals.assert_called_once()

    def test_discovers_jobs_from_modules_when_jobs_not_passed(self, tmp_path: Any) -> None:
        cfg = {"app": {"packages": ["acme_worker_fixture"]}}
        result = self._run(tmp_path, extra_cfg=cfg, jobs=None)
        expected = f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}"
        assert expected in result.celery_app.tasks

    def test_warns_and_starts_when_no_job_is_discovered(
        self, tmp_path: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A worker whose first job is still to come starts, with a warning."""
        with caplog.at_level(logging.WARNING, logger="loom.core.discovery.worker"):
            result = self._run(tmp_path, jobs=None)

        assert result.celery_app is not None
        assert "no Job classes discovered" in caplog.text

    def test_discovers_jobs_from_package_when_jobs_not_passed(self, tmp_path: Any) -> None:
        result = self._run(
            tmp_path, extra_cfg={"app": {"packages": ["acme_worker_fixture"]}}, jobs=None
        )

        expected = f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}"
        assert expected in result.celery_app.tasks

    def test_registers_the_discovered_callbacks_without_a_callbacks_argument(
        self, tmp_path: Any
    ) -> None:
        result = self._run(
            tmp_path, extra_cfg={"app": {"packages": ["acme_worker_fixture"]}}, jobs=None
        )

        tasks = result.celery_app.tasks
        assert f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}" in tasks
        assert f"{TASK_CALLBACK_PREFIX}._FixtureCallback" in tasks
        assert f"{TASK_CALLBACK_ERROR_PREFIX}._FixtureCallback" in tasks

    def test_compiles_models_under_the_configured_naming_convention(self, tmp_path: Any) -> None:
        """The worker names constraints as the REST runtime and the migrations do."""
        from loom.stores.sqlalchemy.schema import reset_registry
        from loom.stores.sqlalchemy.session_manager import SessionManager

        cfg = {
            "app": {"packages": ["acme_worker_fixture"]},
            "stores": {
                "default": {
                    "backend": "sqlalchemy",
                    "url": "sqlite+aiosqlite:///test.db",
                    "schema": {"naming_convention": {"pk": "pk_%(table_name)s"}},
                },
                "settings": {"cursors": {"signing": "ephemeral"}},
            },
        }
        try:
            result = self._run(tmp_path, extra_cfg=cfg, jobs=None)
            metadata = result.container.resolve(SessionManager).metadata
            table = metadata.tables[_DiscoveredModel.__tablename__]
            assert table.primary_key.name == f"pk_{_DiscoveredModel.__tablename__}"
        finally:
            reset_registry(naming_convention=None)

    def test_modules_discovery_includes_autocrud_use_cases_for_models(self, tmp_path: Any) -> None:
        cfg = {"app": {"packages": ["acme_worker_fixture"]}}
        result = self._run(tmp_path, extra_cfg=cfg, jobs=None)
        registry = result.container.resolve(UseCaseRegistry)
        expected_keys = {
            "__discovered_model:create",
            "__discovered_model:get",
            "__discovered_model:list",
        }
        assert expected_keys.issubset(set(registry.keys()))

    def test_autocrud_use_cases_cover_the_five_operations_without_persistence(self) -> None:
        """Workers derive every operation from the model alone; no backend gates them."""
        use_cases = discovery._autocrud_use_cases_from_models((_DiscoveredModel,))

        assert len(use_cases) == 5

    def test_registers_repo_mapping_for_discovered_models(self, tmp_path: Any) -> None:
        cfg = {"app": {"packages": ["acme_worker_fixture"]}}
        result = self._run(tmp_path, extra_cfg=cfg, jobs=None)
        mapped_names = {model.__name__ for model in result.container._repo_bindings}
        assert "_DiscoveredModel" in mapped_names

    def test_starts_without_jobs_or_discovery_with_a_warning(
        self, tmp_path: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="loom.core.discovery.worker"):
            result = self._run(tmp_path, jobs=None)

        assert "no Job classes discovered" in caplog.text
        assert f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}" not in result.celery_app.tasks
