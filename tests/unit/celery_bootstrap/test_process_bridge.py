"""Unit tests for the Celery async runtime bridge."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from loom.celery.config import CeleryRuntimeConfig
from loom.celery.runner import _CeleryProcessBridge
from loom.core.job.job import Job
from tests.helpers.jobs import InlineServiceFixture, JobActivity, JobKernel


class TestCeleryAsyncRuntime:
    def test_initialize_builds_async_bridge_once(self) -> None:
        loop_factory = object()
        runtime = _CeleryProcessBridge(
            backend="asyncio",
            backend_options={"loop_factory": loop_factory},
            shutdown_timeout_ms=10,
        )

        with patch("loom.celery.runner.AsyncBridge") as mock_bridge_cls:
            runtime.initialize()
            runtime.initialize()

        mock_bridge_cls.assert_called_once_with(
            backend="asyncio",
            backend_options={"loop_factory": loop_factory},
            shutdown_timeout_ms=10,
        )

    def test_run_uses_bridge_when_initialized(self) -> None:
        runtime = _CeleryProcessBridge()
        bridge = MagicMock()
        bridge.is_alive = True
        bridge.run = MagicMock(return_value=123)
        runtime._bridge = bridge

        coro = MagicMock()
        result = runtime.run(coro, eager_fallback=False)

        assert result == 123
        bridge.run.assert_called_once()

    def test_run_falls_back_to_asyncio_in_eager_mode(self) -> None:
        runtime = _CeleryProcessBridge()
        coro = AsyncMock(return_value="ok")()

        result = runtime.run(coro, eager_fallback=True)

        assert result == "ok"

    def test_run_closes_and_raises_when_bridge_missing(self) -> None:
        runtime = _CeleryProcessBridge()
        coro = MagicMock()

        with pytest.raises(RuntimeError, match="not initialized"):
            runtime.run(coro, eager_fallback=False)

        coro.close.assert_called_once()

    def test_shutdown_is_idempotent(self) -> None:
        runtime = _CeleryProcessBridge()
        bridge = MagicMock()
        runtime._bridge = bridge

        runtime.shutdown()
        runtime.shutdown()

        bridge.shutdown.assert_called_once()


class ChildJob(Job[None]):
    """Records each run in the activity, after some I/O."""

    def __init__(self, activity: JobActivity) -> None:
        self._activity = activity

    async def execute(self) -> None:
        await asyncio.sleep(0.01)
        self._activity.record("child")


class AcmeTaskError(Exception):
    pass


class TestEagerInlineChildren:
    @pytest.fixture
    def inline_jobs(self, activity: JobActivity) -> InlineServiceFixture:
        kernel = JobKernel()
        kernel.container.register_instance(JobActivity, activity)
        return kernel.serve_inline(ChildJob)

    @pytest.fixture
    def dispatch_then_fail(
        self, inline_jobs: InlineServiceFixture
    ) -> Callable[[], Awaitable[None]]:
        async def dispatch_child_then_raise() -> None:
            await inline_jobs.dispatch_now(ChildJob)
            raise AcmeTaskError

        return dispatch_child_then_raise

    def test_an_eager_task_returns_once_its_inline_child_ran(
        self, inline_jobs: InlineServiceFixture, activity: JobActivity
    ) -> None:
        runtime = _CeleryProcessBridge(jobs=inline_jobs)

        runtime.run(inline_jobs.dispatch_now(ChildJob), eager_fallback=True)

        assert activity.entries == ["child"]

    def test_an_eager_task_that_raises_still_runs_its_inline_child(
        self,
        inline_jobs: InlineServiceFixture,
        dispatch_then_fail: Callable[[], Awaitable[None]],
        activity: JobActivity,
    ) -> None:
        runtime = _CeleryProcessBridge(jobs=inline_jobs)

        with pytest.raises(AcmeTaskError):
            runtime.run(dispatch_then_fail(), eager_fallback=True)

        assert activity.entries == ["child"]


class TestCeleryRuntimeConfig:
    def test_defaults(self) -> None:
        cfg = CeleryRuntimeConfig()
        assert cfg.backend == "asyncio"
        assert cfg.use_uvloop is False
        assert cfg.shutdown_timeout_ms is None
