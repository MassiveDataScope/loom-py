"""Unit tests for CeleryConfig, JobConfig, apply_job_config, and create_celery_app."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import msgspec
import pytest

from loom.celery.config import (
    CeleryConfig,
    CeleryRuntimeConfig,
    JobConfig,
    apply_job_config,
    create_celery_app,
)
from loom.core.async_bridge import build_backend_options as _build_backend_options
from loom.core.config.convert import convert_config
from loom.core.job.job import Job
from loom.core.model.redaction import Dsn

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _JobA(Job[None]):
    __queue__ = "default"
    __retries__ = 0
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self) -> None:
        # Intentional no-op: the test only needs a concrete Job subclass.
        return None


class _JobB(Job[None]):
    __queue__ = "other"
    __retries__ = 1

    def execute(self) -> None:
        # Intentional no-op: the test only needs a concrete Job subclass.
        return None


# ---------------------------------------------------------------------------
# CeleryConfig
# ---------------------------------------------------------------------------


class TestCeleryConfig:
    def test_required_fields(self) -> None:
        cfg = CeleryConfig(
            broker_url=Dsn("redis://localhost/0"), result_backend=Dsn("redis://localhost/1")
        )
        assert cfg.broker_url == "redis://localhost/0"
        assert cfg.result_backend == "redis://localhost/1"

    def test_defaults(self) -> None:
        cfg = CeleryConfig(broker_url=Dsn("r://b"), result_backend=Dsn("r://r"))
        assert cfg.worker_concurrency == 4
        assert cfg.worker_max_tasks_per_child == 1_000
        assert cfg.worker_prefetch_multiplier == 1
        assert cfg.task_always_eager is False
        assert cfg.timezone == "UTC"
        assert cfg.enable_utc is True
        assert cfg.queues == []
        assert cfg.runtime.backend == "asyncio"
        assert cfg.runtime.use_uvloop is False
        assert cfg.runtime.shutdown_timeout_ms is None

    def test_msgspec_convert_from_dict(self) -> None:
        raw = {
            "broker_url": "redis://b",
            "result_backend": "redis://r",
            "worker_concurrency": 8,
            "runtime": {"backend": "trio", "use_uvloop": True, "shutdown_timeout_ms": 1234},
        }
        cfg = convert_config(raw, CeleryConfig, strict=False)
        assert cfg.worker_concurrency == 8
        assert cfg.queues == []
        assert cfg.runtime.backend == "trio"
        assert cfg.runtime.use_uvloop is True
        assert cfg.runtime.shutdown_timeout_ms == 1234

    def test_missing_required_field_raises(self) -> None:
        with pytest.raises(msgspec.ValidationError):
            convert_config({"broker_url": "r://b"}, CeleryConfig, strict=False)

    def test_queues_list_default_not_shared(self) -> None:
        cfg1 = CeleryConfig(broker_url=Dsn("r://b"), result_backend=Dsn("r://r"))
        cfg2 = CeleryConfig(broker_url=Dsn("r://b"), result_backend=Dsn("r://r"))
        assert cfg1.queues is not cfg2.queues


class TestCeleryRuntimeConfig:
    def test_defaults(self) -> None:
        cfg = CeleryRuntimeConfig()
        assert cfg.backend == "asyncio"
        assert cfg.use_uvloop is False
        assert cfg.shutdown_timeout_ms is None

    def test_msgspec_convert_from_dict(self) -> None:
        raw = {"backend": "trio", "use_uvloop": True, "shutdown_timeout_ms": 10}
        cfg = msgspec.convert(raw, CeleryRuntimeConfig, strict=False)
        assert cfg.backend == "trio"
        assert cfg.use_uvloop is True
        assert cfg.shutdown_timeout_ms == 10


class TestBuildBackendOptions:
    def test_non_asyncio_returns_empty(self) -> None:
        assert _build_backend_options("trio", use_uvloop=True) == {}

    def test_asyncio_without_uvloop_returns_empty(self) -> None:
        assert _build_backend_options("asyncio", use_uvloop=False) == {}

    def test_asyncio_with_uvloop_adds_loop_factory(self) -> None:
        import sys

        if sys.platform == "win32":
            pytest.skip("uvloop not available on Windows")

        import uvloop

        opts = _build_backend_options("asyncio", use_uvloop=True)
        assert opts.get("loop_factory") is uvloop.new_event_loop


# ---------------------------------------------------------------------------
# JobConfig
# ---------------------------------------------------------------------------


class TestJobConfig:
    def test_all_fields_default_to_none(self) -> None:
        cfg = JobConfig()
        assert cfg.queue is None
        assert cfg.retries is None
        assert cfg.countdown is None
        assert cfg.timeout is None
        assert cfg.priority is None

    def test_partial_config(self) -> None:
        cfg = JobConfig(queue="heavy", retries=5)
        assert cfg.queue == "heavy"
        assert cfg.retries == 5
        assert cfg.timeout is None

    def test_msgspec_convert_partial(self) -> None:
        cfg = msgspec.convert({"queue": "prices"}, JobConfig, strict=False)
        assert cfg.queue == "prices"
        assert cfg.retries is None


# ---------------------------------------------------------------------------
# apply_job_config
# ---------------------------------------------------------------------------


class TestApplyJobConfig:
    def setup_method(self) -> None:
        # Reset _JobA ClassVars before each test
        _JobA.__queue__ = "default"
        _JobA.__retries__ = 0
        _JobA.__countdown__ = 0
        _JobA.__timeout__ = None
        _JobA.__priority__ = 0

    def test_applies_queue(self) -> None:
        apply_job_config(_JobA, JobConfig(queue="prices.heavy"))
        assert _JobA.__queue__ == "prices.heavy"

    def test_applies_retries(self) -> None:
        apply_job_config(_JobA, JobConfig(retries=5))
        assert _JobA.__retries__ == 5

    def test_applies_timeout(self) -> None:
        apply_job_config(_JobA, JobConfig(timeout=600))
        assert _JobA.__timeout__ == 600

    def test_applies_countdown(self) -> None:
        apply_job_config(_JobA, JobConfig(countdown=30))
        assert _JobA.__countdown__ == 30

    def test_applies_priority(self) -> None:
        apply_job_config(_JobA, JobConfig(priority=9))
        assert _JobA.__priority__ == 9

    def test_empty_config_leaves_classvars_unchanged(self) -> None:
        apply_job_config(_JobA, JobConfig())
        assert _JobA.__queue__ == "default"
        assert _JobA.__retries__ == 0

    def test_partial_config_only_updates_set_fields(self) -> None:
        apply_job_config(_JobA, JobConfig(queue="email"))
        assert _JobA.__queue__ == "email"
        assert _JobA.__retries__ == 0  # unchanged
        assert _JobA.__timeout__ is None  # unchanged

    def test_is_idempotent(self) -> None:
        cfg = JobConfig(retries=3)
        apply_job_config(_JobA, cfg)
        apply_job_config(_JobA, cfg)
        assert _JobA.__retries__ == 3

    def test_does_not_affect_sibling_job(self) -> None:
        original_queue = _JobB.__queue__
        apply_job_config(_JobA, JobConfig(queue="custom"))
        assert _JobB.__queue__ == original_queue

    def test_does_not_modify_job_base_class(self) -> None:
        base_queue = Job.__queue__
        apply_job_config(_JobA, JobConfig(queue="custom"))
        assert Job.__queue__ == base_queue


# ---------------------------------------------------------------------------
# create_celery_app
# ---------------------------------------------------------------------------


class TestCreateCeleryApp:
    def test_sets_broker_url(self) -> None:
        cfg = CeleryConfig(broker_url=Dsn("redis://b:6379/0"), result_backend=Dsn("redis://r:6379/1"))
        mock_celery_cls = MagicMock()
        mock_app = MagicMock()
        mock_celery_cls.return_value = mock_app

        with patch("loom.celery.config.Celery", mock_celery_cls):
            result = create_celery_app(cfg)

        mock_app.conf.update.assert_called_once()
        call_kwargs = mock_app.conf.update.call_args.kwargs
        assert isinstance(call_kwargs["broker_url"], str)
        assert isinstance(call_kwargs["result_backend"], str)
        assert call_kwargs["broker_url"] == "redis://b:6379/0"
        assert call_kwargs["result_backend"] == "redis://r:6379/1"
        assert result is mock_app

    def test_forwards_all_config_fields(self) -> None:
        cfg = CeleryConfig(
            broker_url=Dsn("redis://b/0"),
            result_backend=Dsn("redis://r/1"),
            worker_concurrency=8,
            task_always_eager=True,
            timezone="Europe/Madrid",
        )
        mock_celery_cls = MagicMock()
        mock_app = MagicMock()
        mock_celery_cls.return_value = mock_app

        with patch("loom.celery.config.Celery", mock_celery_cls):
            create_celery_app(cfg)

        kwargs = mock_app.conf.update.call_args.kwargs
        assert kwargs["worker_concurrency"] == 8
        assert kwargs["task_always_eager"] is True
        assert kwargs["timezone"] == "Europe/Madrid"

    def test_queues_empty_does_not_set_task_queues(self) -> None:
        cfg = CeleryConfig(broker_url=Dsn("redis://b/0"), result_backend=Dsn("redis://r/1"))
        mock_celery_cls = MagicMock()
        mock_app = MagicMock()
        mock_celery_cls.return_value = mock_app

        with patch("loom.celery.config.Celery", mock_celery_cls):
            create_celery_app(cfg)

        # task_queues must not be set when queues list is empty
        assert not hasattr(mock_app.conf, "task_queues") or mock_app.conf.task_queues != []

    def test_queues_non_empty_sets_task_queues(self) -> None:
        cfg = CeleryConfig(
            broker_url=Dsn("redis://b/0"),
            result_backend=Dsn("redis://r/1"),
            queues=["default", "heavy"],
        )
        mock_celery_cls = MagicMock()
        mock_app = MagicMock()
        mock_celery_cls.return_value = mock_app

        with (
            patch("loom.celery.config.Celery", mock_celery_cls),
            patch("loom.celery.config.Queue") as mock_queue_cls,
        ):
            create_celery_app(cfg)

        assert mock_queue_cls.call_count == 2
        mock_queue_cls.assert_any_call("default")
        mock_queue_cls.assert_any_call("heavy")
