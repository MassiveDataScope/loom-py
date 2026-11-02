"""The Lambda handler opens its application once at Init and keeps it across warm invocations."""

from __future__ import annotations

import asyncio
import dataclasses
import os
import subprocess
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from loom.aws_lambda import LambdaHTTPAdapter, LambdaHTTPHandler, LambdaUnservedConfigError
from loom.composition import (
    Adapter,
    Application,
    CompositionErrorCode,
    CompositionStateError,
    build_application,
    create_app,
    load_source,
)
from loom.core.config.errors import ConfigError
from loom.core.persistence import LAMBDA_PROFILE
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.unit.aws_lambda.http._contract_app import PUBLIC_PATH, bearer, seen_by
from tests.unit.aws_lambda.http.conftest import lambda_client

pytestmark = [
    pytest.mark.timeout(60),
    pytest.mark.filterwarnings("ignore:Celery config section is missing"),
]

_REPO_ROOT = Path(__file__).resolve().parents[4]
_WARM_INVOCATIONS = 50
_SQL_ENDPOINT = {
    "connections": {
        "analytics": {
            "backend": "clickhouse",
            "url": "http://localhost:8123",
            "allowed_roles": [],
            "default_role": "role_viz_reader",
            "sql_endpoint": {"enabled": True, "auth": "external"},
        }
    }
}

ProjectFactory = Callable[..., str]


class CountedChecks:
    """Counts the startup checks each resource of an application runs."""

    def __init__(self) -> None:
        self.runs: dict[str, int] = {}

    def wrap(self, application: Application) -> None:
        application.resources = tuple(
            dataclasses.replace(resource, check=self._counted(resource.label, resource.check))
            if resource.check is not None
            else resource
            for resource in application.resources
        )

    def _counted(
        self, label: str, check: Callable[[], Awaitable[None]]
    ) -> Callable[[], Awaitable[None]]:
        async def counted() -> None:
            self.runs[label] = self.runs.get(label, 0) + 1
            await check()

        return counted


@pytest.fixture
def opened_on(monkeypatch: pytest.MonkeyPatch) -> list[asyncio.AbstractEventLoop]:
    """Record the loop every :class:`Application` is opened on."""
    loops: list[asyncio.AbstractEventLoop] = []
    running = Application.running

    @asynccontextmanager
    async def recorded(self: Application, **kwargs: Any) -> AsyncIterator[Application]:
        loops.append(asyncio.get_running_loop())
        async with running(self, **kwargs) as application:
            yield application

    monkeypatch.setattr(Application, "running", recorded)
    return loops


def _invoke_warm(handler: LambdaHTTPHandler, times: int) -> None:
    call = lambda_client(handler)
    for _ in range(times):
        call("POST", "/ops/whoami", headers=bearer("alice"))


def test_warm_invocations_open_the_application_once(
    application_openings: list[str], lambda_handler: LambdaHTTPHandler
) -> None:
    _invoke_warm(lambda_handler, _WARM_INVOCATIONS)

    assert application_openings == ["open"]


def test_closing_the_handler_closes_the_application(
    application_openings: list[str], contract_project: ProjectFactory
) -> None:
    handler = create_app(contract_project(), adapter=LambdaHTTPAdapter(), environment="test")
    _invoke_warm(handler, 2)

    handler.close()

    assert application_openings == ["open", "close"]


def test_warm_invocations_run_the_startup_checks_once(contract_project: ProjectFactory) -> None:
    adapter = LambdaHTTPAdapter()
    application = build_application(contract_project(), adapter=adapter, environment="test")
    checks = CountedChecks()
    checks.wrap(application)
    handler = adapter.bind(application)
    try:
        _invoke_warm(handler, _WARM_INVOCATIONS)
    finally:
        handler.close()

    assert checks.runs == {"store:default": 1}


def test_warm_invocations_run_on_the_loop_the_application_opened_on(
    opened_on: list[asyncio.AbstractEventLoop], contract_project: ProjectFactory
) -> None:
    config = contract_project()
    handler = create_app(config, adapter=LambdaHTTPAdapter(), environment="test")
    try:
        _invoke_warm(handler, _WARM_INVOCATIONS)
    finally:
        handler.close()

    request_loops = {loop for loop, _ in seen_by(config)}
    assert (len(seen_by(config)), request_loops) == (_WARM_INVOCATIONS, set(opened_on))


def test_a_loop_set_between_invocations_does_not_move_the_requests(
    opened_on: list[asyncio.AbstractEventLoop], contract_project: ProjectFactory
) -> None:
    config = contract_project()
    handler = create_app(config, adapter=LambdaHTTPAdapter(), environment="test")
    stray = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(stray)
        _invoke_warm(handler, 1)
    finally:
        handler.close()
        stray.close()

    assert [loop for loop, _ in seen_by(config)] == opened_on


def test_two_invocations_share_neither_identity_nor_session(
    contract_project: ProjectFactory,
) -> None:
    config = contract_project()
    handler = create_app(config, adapter=LambdaHTTPAdapter(), environment="test")
    call = lambda_client(handler)
    try:
        subjects = [
            call("POST", "/ops/whoami", headers=bearer("alice")).json(),
            call("POST", "/ops/whoami", headers=bearer("bob")).json(),
            call("POST", PUBLIC_PATH).json(),
        ]
    finally:
        handler.close()

    first, second, _ = (session for _, session in seen_by(config))
    assert subjects == [{"subject": "alice"}, {"subject": "bob"}, {"subject": ""}]
    assert (first is not None, second is not None, first is not second) == (True, True, True)


def test_a_failure_to_open_fails_the_init(contract_project: ProjectFactory) -> None:
    adapter = LambdaHTTPAdapter()
    application = build_application(contract_project(), adapter=adapter, environment="test")

    async def unreachable() -> None:
        raise ConnectionError("database unreachable")

    application.resources = tuple(
        dataclasses.replace(resource, check=unreachable) for resource in application.resources
    )

    with pytest.raises(ConnectionError, match="database unreachable"):
        adapter.bind(application)


def test_the_recipe_asks_for_one_connection_and_no_pool_metrics_or_agents(
    contract_project: ProjectFactory,
) -> None:
    source = load_source(
        contract_project(),
        config_dir=None,
        environment="test",
        code_path=None,
        resolvers=(),
        caller="test",
    )

    recipe = LambdaHTTPAdapter().recipe(source)

    assert (recipe.profile, recipe.pool_metrics, recipe.agents) == (LAMBDA_PROFILE, None, False)


def test_an_sql_endpoint_is_refused_at_recipe_time(contract_project: ProjectFactory) -> None:
    config = contract_project(sql=_SQL_ENDPOINT)

    with pytest.raises(LambdaUnservedConfigError, match="does not mount SQL endpoints") as raised:
        build_application(config, adapter=LambdaHTTPAdapter(), environment="test")
    assert raised.value.key == "sql.connections.analytics.sql_endpoint"


def test_an_sql_connection_without_an_endpoint_is_served(contract_project: ProjectFactory) -> None:
    connection = dict(_SQL_ENDPOINT["connections"]["analytics"], sql_endpoint={"enabled": False})
    config = contract_project(sql={"connections": {"analytics": connection}})

    application = build_application(config, adapter=LambdaHTTPAdapter(), environment="test")

    assert application.sql.config is not None


def test_an_ai_section_is_refused_at_recipe_time(contract_project: ProjectFactory) -> None:
    config = contract_project(ai={"models": {}})

    with pytest.raises(LambdaUnservedConfigError, match="does not host agents") as raised:
        build_application(config, adapter=LambdaHTTPAdapter(), environment="test")
    assert raised.value.key == "ai"


def test_manifest_agents_are_refused_at_recipe_time(contract_project: ProjectFactory) -> None:
    config = contract_project(agents=("agents/*.yaml",))

    with pytest.raises(LambdaUnservedConfigError, match="manifest declares agents") as raised:
        build_application(config, adapter=LambdaHTTPAdapter(), environment="test")
    assert raised.value.key == "AGENTS"


def test_enabled_metrics_are_refused_at_recipe_time(contract_project: ProjectFactory) -> None:
    config = contract_project(observability={"prometheus": {"enabled": True}})

    with pytest.raises(LambdaUnservedConfigError, match="serves no metrics scrape") as raised:
        build_application(config, adapter=LambdaHTTPAdapter(), environment="test")
    assert raised.value.key == "observability.prometheus.enabled"


@pytest.mark.parametrize("adapter", [FastAPIAdapter, LambdaHTTPAdapter])
def test_an_exclusion_reaching_a_templated_route_aborts_startup(
    contract_project: ProjectFactory, adapter: Callable[[], Adapter[object]]
) -> None:
    config = contract_project(exclude_paths=("/items/everything",))

    with pytest.raises(ConfigError, match=r"served by the route '/items/\{item_id\}'"):
        create_app(config, adapter=adapter(), environment="test")


def test_bind_before_recipe_is_refused() -> None:
    application: Any = object()

    with pytest.raises(CompositionStateError, match="LambdaHTTPAdapter.bind") as raised:
        LambdaHTTPAdapter().bind(application)
    assert raised.value.issue.code is CompositionErrorCode.ADAPTER_RECIPE_MISSING


def test_a_second_recipe_on_the_same_adapter_is_refused(contract_project: ProjectFactory) -> None:
    adapter = LambdaHTTPAdapter()
    build_application(contract_project(), adapter=adapter, environment="test")
    second = contract_project()

    with pytest.raises(CompositionStateError, match="one adapter instance") as raised:
        build_application(second, adapter=adapter, environment="test")
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
