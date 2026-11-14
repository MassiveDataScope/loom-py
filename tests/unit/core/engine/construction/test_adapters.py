"""Every adapter T08 moves builds the use case or job inside its execution.

Each one builds a class whose per-execution :class:`~acme.AcmeSession`
declares ``Caller()``: built outside an execution it raises
``ExecutionRequired``, inside one it receives that execution's identity.
"""

from __future__ import annotations

from typing import Any, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.celery.service import CeleryJobService
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS
from loom.core.identity.wire import encode_identity
from loom.core.job.envelope import JobEnvelope
from loom.core.job.run import run_job
from loom.core.job.service import InlineJobService, _PendingDispatch  # pyright: ignore[reportPrivateUsage]
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestInterface, RestRoute
from tests.unit.core.engine.construction import acme


def _executor() -> RuntimeExecutor:
    return RuntimeExecutor(UseCaseCompiler())


async def test_the_invoker_builds_inside_the_execution() -> None:
    invoker = AppInvoker(
        acme.factory(), _executor(), cast(UseCaseRegistry, None), identity=acme.ALICE
    )

    probe = await invoker.invoke(acme.AcmeProbe)

    assert probe.session.caller is acme.ALICE


async def test_the_inline_job_service_builds_inside_the_execution() -> None:
    job = await InlineJobService(acme.factory(), _executor()).run(acme.AcmeProbeJob)

    assert job.session.caller is ANONYMOUS


async def test_an_inline_dispatch_builds_inside_the_execution() -> None:
    results: list[Any] = []
    dispatch = _PendingDispatch(
        job_type=acme.AcmeProbeJob,
        task_id="acme-task",
        payload={},
        params=None,
        on_success=None,
        on_failure=None,
        result_holder=results,
        factory=acme.factory(),
        executor=_executor(),
        identity=acme.ALICE,
    )

    await dispatch.run()

    assert results[0].session.caller is acme.ALICE


async def test_the_job_runner_builds_inside_the_execution() -> None:
    envelope = JobEnvelope(job="AcmeProbeJob", identity=encode_identity(acme.ALICE))

    job = await run_job(
        envelope,
        acme.AcmeProbeJob,
        factory=acme.factory(),
        executor=_executor(),
        observability=ObservabilityRuntime.noop(),
        message_id=None,
    )

    assert job.session.caller == acme.ALICE


async def test_the_celery_job_service_builds_inside_the_execution() -> None:
    service = CeleryJobService(cast(Any, None), factory=acme.factory(), executor=_executor())

    job = await service.run(acme.AcmeProbeJob)

    assert job.session.caller is ANONYMOUS


class AcmeRouted(UseCase[Any, bool]):
    def __init__(self, session: acme.AcmeSession) -> None:
        self.session = session

    async def execute(self) -> bool:
        return self.session.execution is current_execution() is not None


class AcmeRoutes(RestInterface[bool]):
    prefix = "/acme"
    routes = (RestRoute(use_case=AcmeRouted, method="GET", path="/"),)


def test_a_rest_route_builds_inside_the_execution() -> None:
    compiler = UseCaseCompiler()
    compiler.compile(AcmeRouted)
    factory = acme.factory()
    factory.register(AcmeRouted)
    app = FastAPI()
    routes = RestInterfaceCompiler(compiler).compile(AcmeRoutes)
    bind_interfaces(app, routes, factory, RuntimeExecutor(compiler), ObservabilityRuntime.noop())

    response = TestClient(app).get("/acme/")

    assert response.status_code == 200
    assert response.json() is True
