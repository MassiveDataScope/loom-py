from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import FastAPI
from omegaconf import OmegaConf

import loom.rest.fastapi.auto as auto
from loom.core.config import ConfigContext
from loom.core.observability.config import ObservabilityConfig, PrometheusObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.prometheus.middleware import PrometheusMiddleware
from loom.rest.fastapi.auto import _mount_optional_middlewares, _RestConfig, create_app
from loom.rest.middleware import TraceIdMiddleware

_AUTO = cast(Any, auto)


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
            "observability": {
                "log": {"enabled": False},
                "prometheus": {"enabled": False, "config": {"path": "/metrics"}},
            },
        }
    )
    captured: dict[str, Any] = {}
    runtime = ObservabilityRuntime.noop()

    def _fake_from_yaml(
        cls: type[ConfigContext],
        *config_paths: str,
        resolvers: Any = (),
        binder: Any = None,
    ) -> ConfigContext:
        del config_paths, resolvers, binder, cls
        return ConfigContext.from_dict(cast(dict[str, Any], OmegaConf.to_container(raw)))

    def _fake_from_config(
        cls: type[ObservabilityRuntime], config: ObservabilityConfig
    ) -> ObservabilityRuntime:
        captured["observability_config"] = config
        return runtime

    def _fake_build_bootstrap(
        app_cfg: Any,
        ctx: Any,
        metrics: Any | None = None,
        config_interfaces: Any = (),
        modules: Any = (),
    ) -> tuple[Any, Any, Any, Any]:
        del app_cfg, ctx, metrics, config_interfaces, modules
        result = SimpleNamespace(
            # create_app now verifies every compiled plan's Agent() markers
            # (spec 014, T202) before it registers the agent resolver, so the
            # stub compiler and registry must answer both calls the same way
            # a real, marker-free KernelRuntime would: no plan, no key.
            compiler=SimpleNamespace(get_plan=lambda use_case_type: None),
            registry=SimpleNamespace(key_for=lambda use_case_type: None),
            factory=SimpleNamespace(verify=lambda: None),
            # create_app always registers SqlQueryService, so the stub
            # container must accept register() calls.
            container=SimpleNamespace(register=lambda *args, **kwargs: None),
        )

        @asynccontextmanager
        async def _lifespan() -> AsyncIterator[None]:
            yield

        wiring = SimpleNamespace(lifespan_init=_lifespan, readiness=None)
        python_interfaces = (type("DummyRestInterface", (), {}),)
        discovered = SimpleNamespace(
            use_cases=(object(),),
            interfaces=python_interfaces,
            models=(object(),),
            agent_specs=(),
        )
        return result, wiring, discovered, python_interfaces

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

    monkeypatch.setattr(ConfigContext, "from_yaml", classmethod(_fake_from_yaml))
    monkeypatch.setattr(
        _AUTO.ObservabilityRuntime,
        "from_config",
        classmethod(_fake_from_config),
    )
    monkeypatch.setattr(auto, "_build_bootstrap", _fake_build_bootstrap)
    monkeypatch.setattr(auto, "_configure_job_service", _fake_configure_job_service)
    monkeypatch.setattr(auto, "create_fastapi_app", _fake_create_fastapi_app)
    monkeypatch.setattr(auto, "_ensure_code_path", lambda code_path: None)

    app = create_app("config.yaml")

    assert captured["observability_config"].log.enabled is False
    assert captured["runtime"] is runtime
    classes: list[Any] = [m.cls for m in app.user_middleware]
    assert TraceIdMiddleware in classes
    assert PrometheusMiddleware not in classes
