"""Unit tests for loom.celery.bootstrap (Piece 8)."""

from __future__ import annotations

import dataclasses
import logging
import sys
import types
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

import loom.celery._discovery as discovery
import loom.celery.adapter as adapter
import loom.celery.bootstrap as boot
from loom.celery._discovery import _apply_job_config_if_present
from loom.celery.bootstrap import WorkerBootstrapResult, WorkerManifest
from loom.celery.config import CeleryRuntimeConfig
from loom.celery.constants import TASK_CALLBACK_ERROR_PREFIX, TASK_CALLBACK_PREFIX, TASK_JOB_PREFIX
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.registry import UseCaseRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _SyncJob(Job[int]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self, value: int = 0) -> int:
        return value * 2


class _DiscoveredModel(BaseModel):
    __tablename__ = "discovered_model_bootstrap_test"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)


class _ObservedCallback:
    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        del job_id, result, context

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        del job_id, exc_type, exc_msg, context


def _make_raw_config(extra: dict[str, Any] | None = None) -> Any:
    """Build a minimal omegaconf DictConfig for bootstrap_worker tests."""
    from omegaconf import OmegaConf

    base = {
        "celery": {
            "broker_url": "memory://",
            "result_backend": "cache+memory://",
            "task_always_eager": True,
        },
        "database": {"url": "sqlite+aiosqlite:///test.db"},
        "persistence": {"cursors": {"signing": "ephemeral"}},
    }
    if extra:
        base.update(extra)
    return OmegaConf.create(base)


# ---------------------------------------------------------------------------
# _apply_job_config_if_present
# ---------------------------------------------------------------------------


class TestApplyJobConfigIfPresent:
    def test_applies_override_when_section_exists(self, monkeypatch: pytest.MonkeyPatch) -> None:
        raw = _make_raw_config(extra={"jobs": {"_SyncJob": {"queue": "override", "retries": 5}}})
        monkeypatch.setattr(_SyncJob, "__queue__", _SyncJob.__queue__, raising=False)
        monkeypatch.setattr(_SyncJob, "__retries__", _SyncJob.__retries__, raising=False)

        _apply_job_config_if_present(raw, _SyncJob)

        assert _SyncJob.__queue__ == "override"
        assert _SyncJob.__retries__ == 5

    def test_does_not_raise_when_section_absent(self) -> None:
        raw = _make_raw_config()
        # Must not raise even though jobs._SyncJob section is missing
        _apply_job_config_if_present(raw, _SyncJob)

    def test_classvar_unchanged_when_section_absent(self) -> None:
        raw = _make_raw_config()
        original = _SyncJob.__queue__
        _apply_job_config_if_present(raw, _SyncJob)
        assert _SyncJob.__queue__ == original


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
            "database": {"url": "sqlite+aiosqlite:///test.db"},
            "persistence": {"cursors": {"signing": "ephemeral"}},
        }
        if extra_cfg:
            cfg.update(extra_cfg)

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
            "celery": {
                "broker_url": "memory://",
                "result_backend": "cache+memory://",
                "task_always_eager": True,
                "runtime": runtime_cfg,
            },
            "database": {"url": "sqlite+aiosqlite:///test.db"},
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

    def test_applies_job_config_before_registration(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """JobConfig overrides must be applied before task registration.

        Verifies ordering by spying on _make_job_task and capturing the value
        of job_type.__retries__ at the moment of registration.
        """
        import loom.celery.runner as _runner

        monkeypatch.setattr(_SyncJob, "__retries__", _SyncJob.__retries__, raising=False)
        captured_retries: list[int] = []

        def _spy(celery_app: Any, job_type: Any, *args: Any, **kwargs: Any) -> Any:
            captured_retries.append(job_type.__retries__)
            return _runner._make_job_task(celery_app, job_type, *args, **kwargs)

        with patch.object(adapter, "_make_job_task", side_effect=_spy):
            self._run(tmp_path, extra_cfg={"jobs": {"_SyncJob": {"retries": 7}}})

        assert captured_retries == [7], (
            f"_make_job_task saw __retries__={captured_retries}; expected [7]"
        )
        assert _SyncJob.__retries__ == 7

    def test_pure_job_bootstrap_without_database_section(self, tmp_path: Any) -> None:
        """Database config is optional for jobs that do not access repositories."""
        import yaml

        cfg: dict[str, Any] = {
            "celery": {
                "broker_url": "memory://",
                "result_backend": "cache+memory://",
                "task_always_eager": True,
            }
        }
        config_path = tmp_path / "worker_no_db.yaml"
        config_path.write_text(yaml.dump(cfg))

        with patch.object(adapter, "_connect_lifecycle_signals") as mock_signals:
            result = boot.bootstrap_worker(str(config_path), jobs=[_SyncJob])

        assert isinstance(result, WorkerBootstrapResult)
        mock_signals.assert_called_once()

    def test_discovers_jobs_from_modules_when_jobs_not_passed(self, tmp_path: Any) -> None:
        cfg = {
            "app": {
                "discovery": {
                    "mode": "modules",
                    "modules": {"include": ["tests.unit.celery_bootstrap.test_bootstrap"]},
                }
            }
        }
        result = self._run(tmp_path, extra_cfg=cfg)
        expected = f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}"
        assert expected in result.celery_app.tasks

    def test_warns_and_starts_when_no_job_is_discovered(
        self, tmp_path: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A worker whose first job is still to come starts, with a warning."""
        module_name = "tests.unit.celery_bootstrap._manifest_without_jobs_for_test"
        module = types.ModuleType(module_name)
        cast(Any, module).MODELS = [_DiscoveredModel]
        cast(Any, module).JOBS = []
        sys.modules[module_name] = module

        cfg = {"app": {"discovery": {"mode": "manifest", "manifest": {"module": module_name}}}}
        try:
            with caplog.at_level(logging.WARNING, logger="loom.celery.bootstrap"):
                result = self._run(tmp_path, extra_cfg=cfg, jobs=None)
        finally:
            sys.modules.pop(module_name, None)

        assert result.celery_app is not None
        assert "no Job classes discovered" in caplog.text

    def test_discovers_jobs_from_manifest_when_jobs_not_passed(self, tmp_path: Any) -> None:
        module_name = "tests.unit.celery_bootstrap._manifest_jobs_for_test"
        module = types.ModuleType(module_name)
        module_any = cast(Any, module)
        module_any.MANIFEST = WorkerManifest(jobs=[_SyncJob])
        sys.modules[module_name] = module

        cfg = {"app": {"discovery": {"mode": "manifest", "manifest": {"module": module_name}}}}
        try:
            result = self._run(tmp_path, extra_cfg=cfg)
        finally:
            sys.modules.pop(module_name, None)

        expected = f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}"
        assert expected in result.celery_app.tasks

    def test_discovers_jobs_from_manifest_lists_when_manifest_struct_missing(
        self, tmp_path: Any
    ) -> None:
        module_name = "tests.unit.celery_bootstrap._manifest_lists_for_test"
        module = types.ModuleType(module_name)
        module_any = cast(Any, module)
        module_any.JOBS = [_SyncJob]
        module_any.CALLBACKS = [_ObservedCallback]
        sys.modules[module_name] = module

        cfg = {"app": {"discovery": {"mode": "manifest", "manifest": {"module": module_name}}}}
        try:
            result = self._run(tmp_path, extra_cfg=cfg)
        finally:
            sys.modules.pop(module_name, None)

        assert f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}" in result.celery_app.tasks
        assert f"{TASK_CALLBACK_PREFIX}.{_ObservedCallback.__qualname__}" in result.celery_app.tasks
        assert (
            f"{TASK_CALLBACK_ERROR_PREFIX}.{_ObservedCallback.__qualname__}"
            in result.celery_app.tasks
        )

    def test_compiles_models_under_the_configured_naming_convention(self, tmp_path: Any) -> None:
        """The worker names constraints as the REST runtime and the migrations do."""
        from loom.core.backend.sqlalchemy import get_metadata, reset_registry

        module_name = "tests.unit.celery_bootstrap._manifest_naming_for_test"
        module = types.ModuleType(module_name)
        cast(Any, module).MODELS = [_DiscoveredModel]
        cast(Any, module).JOBS = [_SyncJob]
        sys.modules[module_name] = module
        cfg = {
            "app": {"discovery": {"mode": "manifest", "manifest": {"module": module_name}}},
            "database": {
                "url": "sqlite+aiosqlite:///test.db",
                "schema": {"naming_convention": {"pk": "pk_%(table_name)s"}},
            },
        }
        try:
            self._run(tmp_path, extra_cfg=cfg, jobs=None)
            table = get_metadata().tables[_DiscoveredModel.__tablename__]
            assert table.primary_key.name == f"pk_{_DiscoveredModel.__tablename__}"
        finally:
            sys.modules.pop(module_name, None)
            reset_registry(naming_convention=None)

    def test_discovers_callbacks_from_modules_when_jobs_not_passed(self, tmp_path: Any) -> None:
        cfg = {
            "app": {
                "discovery": {
                    "mode": "modules",
                    "modules": {"include": ["tests.unit.celery_bootstrap.test_bootstrap"]},
                }
            }
        }
        result = self._run(tmp_path, extra_cfg=cfg, jobs=None)
        assert f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}" in result.celery_app.tasks
        assert f"{TASK_CALLBACK_PREFIX}.{_ObservedCallback.__qualname__}" in result.celery_app.tasks
        assert (
            f"{TASK_CALLBACK_ERROR_PREFIX}.{_ObservedCallback.__qualname__}"
            in result.celery_app.tasks
        )

    def test_modules_discovery_includes_autocrud_use_cases_for_models(self, tmp_path: Any) -> None:
        cfg = {
            "app": {
                "discovery": {
                    "mode": "modules",
                    "modules": {"include": ["tests.unit.celery_bootstrap.test_bootstrap"]},
                }
            }
        }
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
        cfg = {
            "app": {
                "discovery": {
                    "mode": "modules",
                    "modules": {"include": ["tests.unit.celery_bootstrap.test_bootstrap"]},
                }
            }
        }
        result = self._run(tmp_path, extra_cfg=cfg, jobs=None)
        mapped_names = {model.__name__ for model in result.container._repo_bindings}
        assert "_DiscoveredModel" in mapped_names

    def test_raises_when_no_jobs_and_no_discovery(self, tmp_path: Any) -> None:
        with pytest.raises(RuntimeError):
            self._run(tmp_path, jobs=None)
