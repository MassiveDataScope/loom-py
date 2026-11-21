"""What the Lambda HTTP adapter composes, what it refuses, and what it never loads."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.composition import (
    CompositionErrorCode,
    CompositionStateError,
    Project,
    Recipe,
    Runtime,
    create_app,
)
from loom.core.config.errors import ConfigError
from loom.core.stores import LAMBDA_PROFILE
from loom.rest.compiler import InterfaceCompilationError
from loom.rest.fastapi import FastAPIRuntime
from loom.serverless.aws.lambda_ import LambdaRuntime, LambdaUnservedConfigError
from tests.helpers.lambda_failures import lambda_failure
from tests.unit.serverless.aws.lambda_.http._clients import LambdaProject, lambda_client

pytestmark = [
    pytest.mark.timeout(60),
    pytest.mark.filterwarnings("ignore:Celery config section is missing"),
]

_TEST = "test"
_SQL_CONNECTION: dict[str, Any] = {
    "backend": "clickhouse",
    "url": "http://localhost:8123",
    "allowed_roles": [],
    "default_role": "role_viz_reader",
    "sql_endpoint": {"enabled": True, "auth": "external"},
}
_CONFIG_REFUSALS = [
    pytest.param(
        {"sql": {"connections": {"analytics": _SQL_CONNECTION}}},
        "sql.connections.analytics.sql_endpoint",
        id="sql-endpoint",
    ),
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
    project = Project.load(
        contract_project(),
        config_dir=None,
        environment=_TEST,
        code_path=None,
        resolvers=(),
        caller=_TEST,
    )
    return LambdaRuntime().recipe(project)


class TestLambdaRecipe:
    @pytest.mark.parametrize(
        ("field", "expected"),
        [
            ("profile", LAMBDA_PROFILE),
            ("pool_metrics", None),
            ("agents", True),
            ("agent_endpoints", False),
        ],
    )
    def test_the_recipe_asks_for_one_connection_and_no_pool_metrics_or_agent_endpoints(
        self, recipe: Recipe, field: str, expected: object
    ) -> None:
        assert getattr(recipe, field) == expected

    @pytest.mark.parametrize(("options", "key"), _CONFIG_REFUSALS)
    def test_what_lambda_does_not_serve_is_refused_at_recipe_time(
        self, contract_project: ProjectFactory, options: dict[str, Any], key: str
    ) -> None:
        config = contract_project(**options)

        adapter = LambdaRuntime()

        with pytest.raises(LambdaUnservedConfigError) as raised:
            Project.load(config, environment=_TEST).compose(adapter)
        assert raised.value.key == key

    def test_an_sql_connection_without_an_endpoint_composes_with_its_sql(
        self,
        contract_project: ProjectFactory,
    ) -> None:
        connection = dict(_SQL_CONNECTION, sql_endpoint={"enabled": False})
        config = contract_project(sql={"connections": {"analytics": connection}})

        application = Project.load(config, environment=_TEST).compose(LambdaRuntime())

        assert application.sql.config is not None

    @pytest.mark.parametrize("adapter", [FastAPIRuntime, LambdaRuntime])
    def test_an_exclusion_reaching_a_templated_route_aborts_startup(
        self, contract_project: ProjectFactory, adapter: Callable[[], Runtime[object]]
    ) -> None:
        config = contract_project(exclude_paths=("/items/everything",))
        composed = adapter()
        raises = lambda_failure if adapter is LambdaRuntime else pytest.raises

        with raises(ConfigError, match=r"served by the route '/items/\{item_id\}'"):
            create_app(config, runtime=composed, environment=_TEST)

    def test_a_route_at_a_default_exclusion_requires_a_token_on_lambda(
        self,
        open_lambda: Callable[..., LambdaProject],
    ) -> None:
        project = open_lambda(exclude_paths=None, health_route=True)

        reply = lambda_client(project.handler)("POST", "/health")

        assert reply.status == 401

    def test_fastapi_refuses_a_route_at_its_health_path(
        self, contract_project: ProjectFactory
    ) -> None:
        config = contract_project(exclude_paths=None, health_route=True)
        adapter = FastAPIRuntime()

        with pytest.raises(InterfaceCompilationError, match="reserved for the readiness route"):
            create_app(config, runtime=adapter, environment=_TEST)

    def test_mount_before_recipe_is_refused(self) -> None:
        application: Any = object()
        adapter = LambdaRuntime()

        with pytest.raises(CompositionStateError, match="LambdaRuntime.mount") as raised:
            adapter.mount(application)
        assert raised.value.issue.code is CompositionErrorCode.RUNTIME_RECIPE_MISSING

    def test_a_second_recipe_on_the_same_runtime_is_refused(
        self, contract_project: ProjectFactory
    ) -> None:
        adapter = LambdaRuntime()
        Project.load(contract_project(), environment=_TEST).compose(adapter)
        second = contract_project()

        with pytest.raises(CompositionStateError, match="one runtime instance") as raised:
            Project.load(second, environment=_TEST).compose(adapter)
        assert raised.value.issue.code is CompositionErrorCode.RUNTIME_ALREADY_COMPOSED
