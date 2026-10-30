"""The Celery signals check, open and close the worker's application (US4, T027).

Prefork: ``worker_init`` checks once in the parent and opens nothing; each
child opens with ``check=False`` in ``worker_process_init``. Solo and threads:
``worker_init`` checks and opens. Either shutdown signal closes, idempotently
and best effort.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest
from celery.concurrency.prefork import TaskPool as PreforkPool
from celery.exceptions import WorkerTerminate
from celery.signals import (
    worker_init,
    worker_process_init,
    worker_process_shutdown,
    worker_shutdown,
)

from loom.celery.adapter import CeleryAdapter, _connect_lifecycle_signals, _WorkerLifecycle
from loom.celery.constants import TASK_JOB_PREFIX
from loom.celery.errors import WorkerNotOpenError
from loom.celery.runner import _CeleryAsyncRuntime
from loom.composition import AppConfig, Application, CompositionSource, compose_application
from loom.core.config import ConfigContext
from loom.core.environment import Environment
from loom.core.job.job import Job
from loom.core.observability.runtime import ObservabilityRuntime

_SIGNALS = (worker_init, worker_process_init, worker_process_shutdown, worker_shutdown)


@pytest.fixture(autouse=True)
def isolated_celery_signals() -> Iterator[None]:
    """Save and restore the four lifecycle signals' receivers around each test."""
    saved = [list(cast(list[Any], signal.receivers)) for signal in _SIGNALS]
    for signal in _SIGNALS:
        cast(list[Any], signal.receivers)[:] = []
    yield
    for signal, receivers in zip(_SIGNALS, saved, strict=True):
        cast(list[Any], signal.receivers)[:] = receivers


class _ApplicationSpy:
    """Records what the lifecycle asks of an application."""

    def __init__(
        self, *, check_error: Exception | None = None, open_error: Exception | None = None
    ) -> None:
        self.events: list[str] = []
        self.close_error: Exception | None = None
        self._check_error = check_error
        self._open_error = open_error

    async def check(self) -> None:
        self.events.append("check")
        if self._check_error is not None:
            raise self._check_error

    @asynccontextmanager
    async def running(self, *, check: bool = True) -> AsyncIterator[_ApplicationSpy]:
        self.events.append(f"open(check={check})")
        if self._open_error is not None:
            raise self._open_error
        try:
            yield self
        finally:
            self.events.append("close")
            if self.close_error is not None:
                raise self.close_error


class _RuntimeSpy:
    """A bridge double that runs nothing, so a failure to run can be forced."""

    def __init__(self) -> None:
        self.initialize_calls = 0
        self.shutdown_calls = 0
        self.ran: list[Any] = []
        self.run_error: Exception | None = None

    def initialize(self) -> None:
        self.initialize_calls += 1

    def refuse(self, cause: BaseException) -> None:
        raise AssertionError(f"unexpected refusal: {cause!r}")

    def run(self, coro: Any, *, eager_fallback: bool) -> Any:
        assert eager_fallback is False
        self.ran.append(coro)
        if self.run_error is not None:
            raise self.run_error
        coro.close()
        return None

    def shutdown(self) -> None:
        self.shutdown_calls += 1


def _connect(application: _ApplicationSpy, runtime: Any | None = None) -> Any:
    bridge = runtime if runtime is not None else _CeleryAsyncRuntime()
    _connect_lifecycle_signals(_WorkerLifecycle(cast(Any, application), bridge))
    return bridge


class _Worker:
    """What ``worker_init`` receives: the controller, its pool not yet resolved."""

    def __init__(self, pool: object) -> None:
        self.pool_cls = pool


def _worker(pool: object) -> _Worker:
    return _Worker(pool)


class TestPrefork:
    @pytest.mark.parametrize("pool", ["prefork", "processes", PreforkPool])
    def test_worker_init_checks_once_and_opens_nothing(self, pool: object) -> None:
        application = _ApplicationSpy()
        bridge = _connect(application)

        worker_init.send(sender=_worker(pool))

        assert application.events == ["check"]
        assert bridge._bridge is None

    def test_worker_process_init_starts_the_bridge_and_opens_without_checking(self) -> None:
        application = _ApplicationSpy()
        bridge = _connect(application)
        worker_init.send(sender=_worker("prefork"))

        worker_process_init.send(sender=None)
        try:
            assert bridge._bridge is not None and bridge._bridge.is_alive
            assert application.events == ["check", "open(check=False)"]
        finally:
            worker_process_shutdown.send(sender=None)

    def test_worker_process_shutdown_closes_and_stops_the_bridge(self) -> None:
        application = _ApplicationSpy()
        bridge = _connect(application)
        worker_process_init.send(sender=None)

        worker_process_shutdown.send(sender=None)

        assert application.events == ["open(check=False)", "close"]
        assert bridge._bridge is None

    def test_a_failed_check_stops_the_worker(self) -> None:
        application = _ApplicationSpy(check_error=RuntimeError("schema missing"))
        _connect(application)

        with pytest.raises(WorkerTerminate) as caught:
            worker_init.send(sender=_worker("prefork"))

        assert isinstance(caught.value.__cause__, RuntimeError)

    def test_the_parent_closes_nothing_it_never_opened(self) -> None:
        application = _ApplicationSpy()
        runtime = _RuntimeSpy()
        _connect(application, runtime)
        worker_init.send(sender=_worker("prefork"))

        worker_shutdown.send(sender=None)

        assert application.events == ["check"]
        assert runtime.ran == []
        assert runtime.shutdown_calls == 1


class TestSingleProcessPools:
    @pytest.mark.parametrize("pool", ["solo", "threads"])
    def test_worker_init_checks_and_opens(self, pool: str) -> None:
        application = _ApplicationSpy()
        bridge = _connect(application)

        worker_init.send(sender=_worker(pool))
        try:
            assert application.events == ["open(check=True)"]
            assert bridge._bridge is not None and bridge._bridge.is_alive
        finally:
            worker_shutdown.send(sender=None)

        assert application.events == ["open(check=True)", "close"]
        assert bridge._bridge is None

    def test_a_failed_open_stops_the_worker(self) -> None:
        application = _ApplicationSpy(open_error=RuntimeError("schema missing"))
        bridge = _connect(application)

        with pytest.raises(WorkerTerminate) as caught:
            worker_init.send(sender=_worker("solo"))

        assert isinstance(caught.value.__cause__, RuntimeError)
        assert bridge._bridge is None


class _PingJob(Job[int]):
    __retries__ = 0

    def execute(self) -> int:
        return 1


class TestFailedChildOpen:
    """Celery swallows a failure in ``worker_process_init``; the child must not serve."""

    def _worker(self, tmp_path: Path) -> Any:
        adapter = CeleryAdapter(jobs=[_PingJob])
        source = CompositionSource(
            config=ConfigContext.from_dict(
                {
                    "celery": {
                        "broker_url": "memory://",
                        "result_backend": "cache+memory://",
                        "task_always_eager": True,
                    }
                }
            ),
            environment=Environment.TEST,
            code_path=tmp_path,
            config_root=tmp_path,
            app=AppConfig(name="worker"),
            observability=ObservabilityRuntime.noop(),
        )
        application = compose_application(source, adapter)

        @asynccontextmanager
        async def _failing(*, check: bool = True) -> AsyncIterator[Application]:
            del check
            raise RuntimeError("clickhouse unreachable")
            yield application

        cast(Any, application).running = _failing
        celery_app = adapter.bind(application)
        return celery_app.tasks[f"{TASK_JOB_PREFIX}.{_PingJob.__qualname__}"]

    def test_every_task_fails_fast_naming_the_failure_to_open(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        task = self._worker(tmp_path)

        with caplog.at_level(logging.ERROR, logger="loom.celery.adapter"):
            worker_process_init.send(sender=None)
            failures = [task.apply().result for _ in range(2)]

        for failure in failures:
            assert isinstance(failure, WorkerNotOpenError)
            assert isinstance(failure.__cause__, RuntimeError)
            assert failure.cause is failure.__cause__
        opening = [r for r in caplog.records if "failed to open its application" in r.message]
        assert len(opening) == 1


class TestClosing:
    def test_both_shutdown_signals_close_once(self) -> None:
        application = _ApplicationSpy()
        _connect(application)
        worker_process_init.send(sender=None)

        worker_process_shutdown.send(sender=None)
        worker_shutdown.send(sender=None)

        assert application.events.count("close") == 1

    def test_a_failure_to_close_is_logged_and_still_stops_the_bridge(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        application = _ApplicationSpy()
        application.close_error = RuntimeError("pool gone")
        bridge = _connect(application)
        worker_process_init.send(sender=None)

        with caplog.at_level(logging.WARNING, logger="loom.celery.adapter"):
            worker_process_shutdown.send(sender=None)

        assert "pool gone" in caplog.text
        assert bridge._bridge is None

    def test_a_bridge_that_cannot_run_the_close_still_shuts_down(self) -> None:
        application = _ApplicationSpy()
        runtime = _RuntimeSpy()
        _connect(application, runtime)
        worker_process_init.send(sender=None)
        runtime.run_error = RuntimeError("loop stopped")

        worker_process_shutdown.send(sender=None)

        exit_coroutine = runtime.ran[-1]
        assert inspect.getcoroutinestate(exit_coroutine) == inspect.CORO_CLOSED
        assert runtime.shutdown_calls == 1


class TestConnecting:
    def test_a_second_connect_replaces_the_first(self) -> None:
        first, second = _ApplicationSpy(), _ApplicationSpy()
        _connect(first)
        _connect(second)

        worker_init.send(sender=_worker("prefork"))

        assert first.events == []
        assert second.events == ["check"]
