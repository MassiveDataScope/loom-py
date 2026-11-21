"""The Lambda handler opens its application once at Init and keeps it across warm invocations."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from loom.composition import Application, Project
from loom.core.job.job import Job
from loom.serverless.aws.lambda_ import LambdaRuntime
from loom.serverless.aws.lambda_.handler import LambdaHandler
from loom.serverless.aws.lambda_.triggers.http import LambdaHTTPHandler
from loom.testing import LambdaTestClient
from tests.helpers.composition import application_of
from tests.helpers.jobs import InlineServiceFixture, JobActivity, JobKernel
from tests.helpers.lambda_failures import lambda_failure
from tests.helpers.loops import RecordedLoops
from tests.unit.serverless.aws.lambda_.http._clients import LambdaProject, Reply, lambda_client
from tests.unit.serverless.aws.lambda_.http._contract_app import PUBLIC_PATH, bearer, seen_by

pytestmark = [
    pytest.mark.timeout(60),
    pytest.mark.filterwarnings("ignore:Celery config section is missing"),
]

_WARM_INVOCATIONS = 50
_WHOAMI = "/ops/whoami"

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


@pytest.fixture
def unreachable(contract_project: ProjectFactory) -> tuple[LambdaRuntime, Application]:
    """A composed application whose database is unreachable when it opens."""
    adapter = LambdaRuntime()
    application = Project.load(contract_project(), environment="test").compose(adapter)

    async def refuse() -> None:
        raise ConnectionError("database unreachable")

    application.resources = tuple(
        dataclasses.replace(resource, check=refuse) for resource in application.resources
    )
    return adapter, application


@pytest.fixture
def three_callers(lambda_project: LambdaProject) -> list[Reply]:
    """Alice, Bob and an anonymous caller, one invocation each."""
    call = lambda_client(lambda_project.handler)
    return [
        call("POST", _WHOAMI, headers=bearer("alice")),
        call("POST", _WHOAMI, headers=bearer("bob")),
        call("POST", PUBLIC_PATH),
    ]


def _invoke_warm(handler: LambdaHandler, times: int) -> None:
    call = lambda_client(handler)
    for _ in range(times):
        call("POST", _WHOAMI, headers=bearer("alice"))


class TestLambdaHandler:
    def test_warm_invocations_open_the_application_once(
        self, application_openings: list[str], lambda_project: LambdaProject
    ) -> None:
        _invoke_warm(lambda_project.handler, _WARM_INVOCATIONS)

        assert application_openings == ["open"]

    def test_closing_the_handler_closes_the_application(
        self, application_openings: list[str], lambda_project: LambdaProject
    ) -> None:
        _invoke_warm(lambda_project.handler, 2)

        lambda_project.handler.close()

        assert application_openings == ["open", "close"]

    def test_warm_invocations_run_the_startup_checks_once(
        self, contract_project: ProjectFactory
    ) -> None:
        adapter = LambdaRuntime()
        application = Project.load(contract_project(), environment="test").compose(adapter)
        checks = CountedChecks()
        checks.wrap(application)
        handler = adapter.mount(application)
        try:
            _invoke_warm(handler, _WARM_INVOCATIONS)
        finally:
            handler.close()

        assert checks.runs == {"store:default": 1}

    def test_every_warm_invocation_is_served(self, lambda_project: LambdaProject) -> None:
        _invoke_warm(lambda_project.handler, _WARM_INVOCATIONS)

        assert len(seen_by(lambda_project.config)) == _WARM_INVOCATIONS

    def test_warm_invocations_run_on_the_loop_the_application_opened_on(
        self, opened_on: list[asyncio.AbstractEventLoop], lambda_project: LambdaProject
    ) -> None:
        _invoke_warm(lambda_project.handler, _WARM_INVOCATIONS)

        assert {loop for loop, _ in seen_by(lambda_project.config)} == set(opened_on)

    def test_a_loop_set_between_invocations_does_not_move_the_requests(
        self, opened_on: list[asyncio.AbstractEventLoop], lambda_project: LambdaProject
    ) -> None:
        stray = asyncio.new_event_loop()
        asyncio.set_event_loop(stray)
        try:
            _invoke_warm(lambda_project.handler, 1)
        finally:
            stray.close()

        assert [loop for loop, _ in seen_by(lambda_project.config)] == opened_on

    def test_each_invocation_sees_its_own_identity(self, three_callers: list[Reply]) -> None:
        assert [reply.json() for reply in three_callers] == [
            {"subject": "alice"},
            {"subject": "bob"},
            {"subject": ""},
        ]

    @pytest.mark.usefixtures("three_callers")
    def test_each_authenticated_invocation_runs_in_a_session(
        self, lambda_project: LambdaProject
    ) -> None:
        first, second, _ = (session for _, session in seen_by(lambda_project.config))

        assert None not in (first, second)

    @pytest.mark.usefixtures("three_callers")
    def test_two_invocations_share_no_session(self, lambda_project: LambdaProject) -> None:
        first, second, _ = (session for _, session in seen_by(lambda_project.config))

        assert first is not second

    def test_the_init_creates_one_loop(
        self, event_loops: RecordedLoops, lambda_project: LambdaProject
    ) -> None:
        lambda_client(lambda_project.handler)("POST", PUBLIC_PATH)

        assert len(event_loops.created) == 1

    @pytest.mark.parametrize("closes", [1, 2], ids=["once", "twice"])
    def test_closing_the_handler_closes_its_loop(
        self, event_loops: RecordedLoops, lambda_project: LambdaProject, closes: int
    ) -> None:
        for _ in range(closes):
            lambda_project.handler.close()

        assert event_loops.created[0].is_closed()

    def test_closing_the_handler_unsets_its_loop(
        self, event_loops: RecordedLoops, lambda_project: LambdaProject
    ) -> None:
        lambda_project.handler.close()

        assert event_loops.made_current[-1] is None

    def test_a_failure_to_open_fails_the_init(
        self,
        unreachable: tuple[LambdaRuntime, Application],
    ) -> None:
        adapter, application = unreachable

        with lambda_failure(ConnectionError, match="database unreachable"):
            adapter.mount(application)

    def test_a_failure_to_open_closes_its_loop(
        self, event_loops: RecordedLoops, unreachable: tuple[LambdaRuntime, Application]
    ) -> None:
        adapter, application = unreachable

        with lambda_failure(ConnectionError):
            adapter.mount(application)
        assert event_loops.created[0].is_closed()

    def test_a_failure_to_open_unsets_its_loop(
        self, event_loops: RecordedLoops, unreachable: tuple[LambdaRuntime, Application]
    ) -> None:
        adapter, application = unreachable

        with lambda_failure(ConnectionError):
            adapter.mount(application)
        assert event_loops.made_current[-1] is None


class RestockedJob(Job[None]):
    """Records each run in the activity, after some I/O."""

    def __init__(self, activity: JobActivity) -> None:
        self._activity = activity

    async def execute(self) -> None:
        await asyncio.sleep(0.01)
        self._activity.record("restocked")


class TestInlineJobsOfAnInvocation:
    @pytest.fixture
    def inline_jobs(self, activity: JobActivity) -> InlineServiceFixture:
        kernel = JobKernel()
        kernel.container.register_instance(JobActivity, activity)
        return kernel.serve_inline(RestockedJob)

    @pytest.fixture
    def client(
        self, inline_jobs: InlineServiceFixture, activity: JobActivity
    ) -> Iterator[LambdaTestClient]:
        async def restock(request: Request) -> JSONResponse:
            await inline_jobs.dispatch_now(RestockedJob)
            return JSONResponse({"ran": list(activity.entries)})

        app = Starlette(routes=[Route("/restock", restock, methods=["POST"])])
        handler = LambdaHTTPHandler(app, application_of(jobs=inline_jobs))
        yield LambdaTestClient(handler)
        handler.close()

    def test_an_invocation_returns_once_the_inline_job_its_route_dispatched_ran(
        self, client: LambdaTestClient, activity: JobActivity
    ) -> None:
        response = client.post("/restock")

        assert (response.json(), activity.entries) == ({"ran": []}, ["restocked"])
