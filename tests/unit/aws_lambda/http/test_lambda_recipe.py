"""What the Lambda HTTP adapter composes, what it refuses, and what it never loads."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from loom.aws_lambda import LambdaHTTPAdapter, LambdaUnservedConfigError
from loom.composition import (
    Adapter,
    CompositionErrorCode,
    CompositionStateError,
    Recipe,
    build_application,
    create_app,
    load_source,
)
from loom.core.config.errors import ConfigError
from loom.core.persistence import LAMBDA_PROFILE
from loom.rest.compiler import InterfaceCompilationError
from loom.rest.fastapi import FastAPIAdapter
from tests.unit.aws_lambda.http._clients import LambdaProject, lambda_client

pytestmark = [
    pytest.mark.timeout(60),
    pytest.mark.filterwarnings("ignore:Celery config section is missing"),
]

_REPO_ROOT = Path(__file__).resolve().parents[4]
_TEST = "test"
_SQL_CONNECTION: dict[str, Any] = {
    "backend": "clickhouse",
    "url": "http://localhost:8123",
    "allowed_roles": [],
    "default_role": "role_viz_reader",
    "sql_endpoint": {"enabled": True, "auth": "external"},
}
_UNDISCOVERABLE_APP = {
    "name": "contract",
    "discovery": {"mode": "interfaces", "interfaces": {"modules": ["no_such_contract_module"]}},
}
_CONFIG_REFUSALS = [
    pytest.param(
        {"sql": {"connections": {"analytics": _SQL_CONNECTION}}},
        "sql.connections.analytics.sql_endpoint",
        id="sql-endpoint",
    ),
    pytest.param({"ai": {"models": {}}}, "ai", id="ai-section"),
    pytest.param(
        {"observability": {"prometheus": {"enabled": True}}},
        "observability.prometheus.enabled",
        id="metrics",
    ),
]
"""What the configuration alone shows Lambda does not serve, and the key each is refused at."""

ProjectFactory = Callable[..., str]


@pytest.fixture
def recipe(contract_project: ProjectFactory) -> Recipe:
    source = load_source(
        contract_project(),
        config_dir=None,
        environment=_TEST,
        code_path=None,
        resolvers=(),
        caller=_TEST,
    )
    return LambdaHTTPAdapter().recipe(source)


@pytest.mark.parametrize(
    ("field", "expected"),
    [("profile", LAMBDA_PROFILE), ("pool_metrics", None), ("agents", False)],
)
def test_the_recipe_asks_for_one_connection_and_no_pool_metrics_or_agents(
    recipe: Recipe, field: str, expected: object
) -> None:
    assert getattr(recipe, field) == expected


@pytest.mark.parametrize(
    ("options", "key"),
    [
        *_CONFIG_REFUSALS,
        pytest.param({"agents": ("agents/*.yaml",)}, "manifest.AGENTS", id="agents"),
    ],
)
def test_what_lambda_does_not_serve_is_refused_at_recipe_time(
    contract_project: ProjectFactory, options: dict[str, Any], key: str
) -> None:
    config = contract_project(**options)

    with pytest.raises(LambdaUnservedConfigError) as raised:
        build_application(config, adapter=LambdaHTTPAdapter(), environment=_TEST)
    assert raised.value.key == key


@pytest.mark.parametrize(("options", "key"), _CONFIG_REFUSALS)
def test_a_configuration_refusal_comes_before_discovery(
    contract_project: ProjectFactory, options: dict[str, Any], key: str
) -> None:
    config = contract_project(app=_UNDISCOVERABLE_APP, **options)

    with pytest.raises(LambdaUnservedConfigError) as raised:
        build_application(config, adapter=LambdaHTTPAdapter(), environment=_TEST)
    assert raised.value.key == key


def test_an_sql_connection_without_an_endpoint_composes_with_its_sql(
    contract_project: ProjectFactory,
) -> None:
    connection = dict(_SQL_CONNECTION, sql_endpoint={"enabled": False})
    config = contract_project(sql={"connections": {"analytics": connection}})

    application = build_application(config, adapter=LambdaHTTPAdapter(), environment=_TEST)

    assert application.sql.config is not None


@pytest.mark.parametrize("adapter", [FastAPIAdapter, LambdaHTTPAdapter])
def test_an_exclusion_reaching_a_templated_route_aborts_startup(
    contract_project: ProjectFactory, adapter: Callable[[], Adapter[object]]
) -> None:
    config = contract_project(exclude_paths=("/items/everything",))

    with pytest.raises(ConfigError, match=r"served by the route '/items/\{item_id\}'"):
        create_app(config, adapter=adapter(), environment=_TEST)


def test_a_route_at_a_default_exclusion_requires_a_token_on_lambda(
    open_lambda: Callable[..., LambdaProject],
) -> None:
    project = open_lambda(exclude_paths=None, health_route=True)

    reply = lambda_client(project.handler)("POST", "/health")

    assert reply.status == 401


def test_fastapi_refuses_a_route_at_its_health_path(contract_project: ProjectFactory) -> None:
    config = contract_project(exclude_paths=None, health_route=True)

    with pytest.raises(InterfaceCompilationError, match="reserved for the readiness route"):
        create_app(config, adapter=FastAPIAdapter(), environment=_TEST)


def test_bind_before_recipe_is_refused() -> None:
    application: Any = object()

    with pytest.raises(CompositionStateError, match="LambdaHTTPAdapter.bind") as raised:
        LambdaHTTPAdapter().bind(application)
    assert raised.value.issue.code is CompositionErrorCode.ADAPTER_RECIPE_MISSING


def test_a_second_recipe_on_the_same_adapter_is_refused(contract_project: ProjectFactory) -> None:
    adapter = LambdaHTTPAdapter()
    build_application(contract_project(), adapter=adapter, environment=_TEST)
    second = contract_project()

    with pytest.raises(CompositionStateError, match="one adapter instance") as raised:
        build_application(second, adapter=adapter, environment=_TEST)
    assert raised.value.issue.code is CompositionErrorCode.ADAPTER_ALREADY_COMPOSED


@pytest.mark.parametrize(
    "module",
    ["loom.composition", "loom.core.persistence", "loom.aws_lambda", "loom.aws_lambda.sqs"],
)
def test_importing_the_transport_neutral_parts_loads_no_http_framework(module: str) -> None:
    probe = (
        f"import sys, {module}; "
        "print(sorted(m for m in ('fastapi', 'starlette', 'mangum') if m in sys.modules))"
    )
    env = {**os.environ, "PYTHONPATH": str(_REPO_ROOT / "src")}

    result = subprocess.run(
        [sys.executable, "-c", probe], env=env, capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == "[]"
