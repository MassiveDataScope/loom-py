"""Unit tests for CeleryConfig, JobConfig and create_celery_app."""

from __future__ import annotations

import msgspec
import pytest

from loom.celery.config import (
    CeleryConfig,
    CeleryRuntimeConfig,
    create_celery_app,
)
from loom.core.config.convert import convert_config
from loom.core.job.config import JobConfig, RetryConfig
from loom.core.model.redaction import Dsn

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


# ---------------------------------------------------------------------------
# JobConfig
# ---------------------------------------------------------------------------


class TestJobConfig:
    def test_all_fields_default_to_unset(self) -> None:
        cfg = JobConfig()
        assert cfg.queue is None
        assert cfg.retry == RetryConfig()
        assert cfg.countdown is None
        assert cfg.timeout is None
        assert cfg.priority is None

    def test_partial_config(self) -> None:
        cfg = JobConfig(queue="heavy", retry=RetryConfig(max_attempts=5))
        assert cfg.queue == "heavy"
        assert cfg.retry.max_attempts == 5
        assert cfg.timeout is None

    def test_msgspec_convert_partial(self) -> None:
        cfg = msgspec.convert({"queue": "prices"}, JobConfig, strict=False)
        assert cfg.queue == "prices"
        assert cfg.retry == RetryConfig()


# ---------------------------------------------------------------------------
# create_celery_app
# ---------------------------------------------------------------------------


class TestCreateCeleryApp:
    def test_the_application_takes_the_broker_and_the_result_backend(self) -> None:
        cfg = CeleryConfig(
            broker_url=Dsn("redis://b:6379/0"), result_backend=Dsn("redis://r:6379/1")
        )

        app = create_celery_app(cfg)

        assert (app.conf.broker_url, app.conf.result_backend) == (
            "redis://b:6379/0",
            "redis://r:6379/1",
        )

    def test_the_application_takes_every_other_field_of_the_config(self) -> None:
        cfg = CeleryConfig(
            broker_url=Dsn("redis://b/0"),
            result_backend=Dsn("redis://r/1"),
            worker_concurrency=8,
            task_always_eager=True,
            timezone="Europe/Madrid",
        )

        app = create_celery_app(cfg)

        assert (app.conf.worker_concurrency, app.conf.task_always_eager, app.conf.timezone) == (
            8,
            True,
            "Europe/Madrid",
        )

    def test_without_queues_celery_creates_them_on_demand(self) -> None:
        cfg = CeleryConfig(broker_url=Dsn("redis://b/0"), result_backend=Dsn("redis://r/1"))

        app = create_celery_app(cfg)

        assert app.conf.task_queues is None

    def test_the_queues_of_the_config_are_declared(self) -> None:
        cfg = CeleryConfig(
            broker_url=Dsn("redis://b/0"),
            result_backend=Dsn("redis://r/1"),
            queues=["default", "heavy"],
        )

        app = create_celery_app(cfg)

        assert [queue.name for queue in app.conf.task_queues] == ["default", "heavy"]

    def test_a_default_queue_that_is_not_declared_is_refused(self) -> None:
        cfg = CeleryConfig(
            broker_url=Dsn("redis://b/0"),
            result_backend=Dsn("redis://r/1"),
            queues=["heavy"],
            task_default_queue="default",
        )

        with pytest.raises(ValueError, match="task_default_queue='default' is not declared"):
            create_celery_app(cfg)
