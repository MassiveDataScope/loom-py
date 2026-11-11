"""Per-route pagination ceilings across compilation, HTTP, and YAML config."""

from __future__ import annotations

import asyncio
from typing import Any

import msgspec
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.repository.abc.query import QuerySpec
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.config import RestInterfaceConfig, build_interfaces_from_config
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestApiDefaults, RestInterface, RestRoute


class ListItems(UseCase[Any, dict[str, int]]):
    read_only = True

    async def execute(self, query: QuerySpec) -> dict[str, int]:
        await asyncio.sleep(0)
        return {"limit": query.limit}


def _client(route_limit: int | None, default_limit: int) -> TestClient:
    class Items(RestInterface[str]):
        prefix = "/items"
        routes = (
            RestRoute(use_case=ListItems, method="GET", path="/")
            if route_limit is None
            else RestRoute(use_case=ListItems, method="GET", path="/", max_limit=route_limit),
        )

    compiler = UseCaseCompiler()
    compiler.compile(ListItems)
    factory = UseCaseFactory(LoomContainer())
    factory.register(ListItems)
    app = FastAPI()
    bind_interfaces(
        app,
        RestInterfaceCompiler(compiler, defaults=RestApiDefaults(max_limit=default_limit)).compile(
            Items
        ),
        factory,
        RuntimeExecutor(compiler),
        observability_runtime=ObservabilityRuntime.noop(),
    )
    return TestClient(app)


def _published_and_applied_limit(client: TestClient) -> tuple[int, int]:
    operation = client.get("/openapi.json").json()["paths"]["/items/"]["get"]
    schema = next(p["schema"] for p in operation["parameters"] if p["name"] == "limit")
    applied = client.get("/items/?limit=500").json()["limit"]
    return schema["maximum"], applied


def test_route_limit_clamps_runtime_and_openapi() -> None:
    assert _published_and_applied_limit(_client(100, 1000)) == (100, 100)


def test_route_limit_cannot_raise_global_ceiling() -> None:
    assert _published_and_applied_limit(_client(300, 200)) == (200, 200)


def test_route_without_limit_inherits_global_ceiling() -> None:
    assert _published_and_applied_limit(_client(None, 200)) == (200, 200)


def test_yaml_route_limit_reaches_python_route() -> None:
    raw = {
        "prefix": "/items",
        "routes": [
            {
                "use_case": f"{__name__}:ListItems",
                "method": "GET",
                "path": "/",
                "max_limit": 100,
            }
        ],
    }
    (interface,) = build_interfaces_from_config(
        {"items": msgspec.convert(raw, RestInterfaceConfig)}
    )
    assert interface.routes[0].max_limit == 100


@pytest.mark.parametrize("invalid", [0, -1, True, 1.5, "100"])
def test_route_limit_requires_positive_int(invalid: Any) -> None:
    with pytest.raises(ValueError, match="max_limit"):
        RestRoute(use_case=ListItems, method="GET", path="/", max_limit=invalid)
