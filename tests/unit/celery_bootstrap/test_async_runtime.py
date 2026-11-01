"""Unit tests for the Celery async runtime bridge."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from loom.celery.config import CeleryRuntimeConfig
from loom.celery.runner import _CeleryAsyncRuntime


class TestCeleryAsyncRuntime:
    def test_initialize_builds_async_bridge_once(self) -> None:
        loop_factory = object()
        runtime = _CeleryAsyncRuntime(
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
        runtime = _CeleryAsyncRuntime()
        bridge = MagicMock()
        bridge.is_alive = True
        bridge.run = MagicMock(return_value=123)
        runtime._bridge = bridge

        coro = MagicMock()
        result = runtime.run(coro, eager_fallback=False)

        assert result == 123
        bridge.run.assert_called_once()

    def test_run_falls_back_to_asyncio_in_eager_mode(self) -> None:
        runtime = _CeleryAsyncRuntime()
        coro = AsyncMock(return_value="ok")()

        result = runtime.run(coro, eager_fallback=True)

        assert result == "ok"

    def test_run_closes_and_raises_when_bridge_missing(self) -> None:
        runtime = _CeleryAsyncRuntime()
        coro = MagicMock()

        with pytest.raises(RuntimeError, match="not initialized"):
            runtime.run(coro, eager_fallback=False)

        coro.close.assert_called_once()

    def test_shutdown_is_idempotent(self) -> None:
        runtime = _CeleryAsyncRuntime()
        bridge = MagicMock()
        runtime._bridge = bridge

        runtime.shutdown()
        runtime.shutdown()

        bridge.shutdown.assert_called_once()


class TestCeleryRuntimeConfig:
    def test_defaults(self) -> None:
        cfg = CeleryRuntimeConfig()
        assert cfg.backend == "asyncio"
        assert cfg.use_uvloop is False
        assert cfg.shutdown_timeout_ms is None
