from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import FastAPI
from omegaconf import OmegaConf

from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
from loom.core.environment import Environment
from loom.core.observability.config import ObservabilityConfig, PrometheusObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.prometheus.middleware import PrometheusMiddleware
from loom.rest.fastapi.adapter import _mount_optional_middlewares, _RestConfig
from loom.rest.fastapi.auto import create_app
from loom.rest.middleware import TraceIdMiddleware


def test_mount_optional_middlewares_always_adds_trace_middleware() -> None:
    app = FastAPI()

    _mount_optional_middlewares(
        app,
        _RestConfig(),
        ObservabilityConfig().prometheus,
        None,
    )

    classes: list[Any] = [m.cls for m in app.user_middleware]
    assert TraceIdMiddleware in classes


def test_mount_optional_middlewares_adds_prometheus_when_enabled() -> None:
    app = FastAPI()

    cfg = PrometheusObservabilityConfig(enabled=True, config=None)
    _mount_optional_middlewares(app, _RestConfig(), cfg, None)

    classes: list[Any] = [m.cls for m in app.user_middleware]
    assert TraceIdMiddleware in classes
    assert PrometheusMiddleware in classes


def test_create_app_uses_observability_section(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = OmegaConf.create(
        {
            "app": {"name": "demo"},
            "database": {"url": "sqlite+aiosqlite:///"},
            # Development refuses an unsigned cursor keyring. The section under
            # test is observability; the opt-out only lets composition finish.
            "persistence": {"cursors": {"signing": "ephemeral"}},
            "observability": {
                "log": {"enabled": False},
                "prometheus": {"enabled": False, "config": {"path": "/metrics"}},
            },
        }
    )
    captured: dict[str, Any] = {}
    runtime = ObservabilityRuntime.noop()

    def _fake_load(
        *config_paths: str, resolvers: Any = (), environment: Any = None
    ) -> ConfigContext:
        del config_paths, resolvers, environment
        return ConfigContext.from_dict(cast(dict[str, Any], OmegaConf.to_container(raw)))

    def _fake_from_config(
        cls: type[ObservabilityRuntime],
        config: ObservabilityConfig,
        *,
        environment: Environment,
    ) -> ObservabilityRuntime:
        captured["observability_config"] = config
        captured["environment"] = environment
        return runtime

    def _fake_configure_job_service(
        raw_cfg: Any, result: Any, observability_runtime: ObservabilityRuntime | None
    ) -> None:
        del raw_cfg, result, observability_runtime

    def _fake_create_fastapi_app(
        result: Any,
        routes: Any,
        *,
        observability_runtime: ObservabilityRuntime | None = None,
        **kwargs: Any,
    ) -> FastAPI:
        del result, routes, kwargs
        captured["runtime"] = observability_runtime
        return FastAPI()

    monkeypatch.setattr("loom.composition.build.load_application_config", _fake_load)
    monkeypatch.setattr(ObservabilityRuntime, "from_config", classmethod(_fake_from_config))
    # The config names no interface module. An empty result lets recipe()
    # continue so create_app still mounts the middlewares under test.
    monkeypatch.setattr(
        "loom.rest.fastapi.adapter._discover_components",
        lambda _source: DiscoveryResult(models=(), use_cases=(), interfaces=()),
    )
    monkeypatch.setattr("loom.composition.build.configure_job_service", _fake_configure_job_service)
    monkeypatch.setattr("loom.rest.fastapi.adapter.create_fastapi_app", _fake_create_fastapi_app)
    monkeypatch.setattr(
        "loom.composition.build.add_code_path",
        lambda code_path, config_dir: Path(code_path),
    )

    app = create_app("config.yaml", environment="development")

    assert captured["observability_config"].log.enabled is False
    assert captured["environment"] is Environment.DEVELOPMENT
    assert captured["runtime"] is runtime
    classes: list[Any] = [m.cls for m in app.user_middleware]
    assert TraceIdMiddleware in classes
    assert PrometheusMiddleware not in classes
