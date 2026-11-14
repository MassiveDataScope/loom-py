"""Every adapter T08 moves builds the use case or job inside its execution (T09 moves these).

Each one builds a class whose per-execution ``AcmeSession`` (``doubles``)
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
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestInterface, RestRoute

from .doubles import ALICE, AcmeProbe, AcmeProbeJob, AcmeSession


class AcmeRouted(UseCase[Any, bool]):
    def __init__(self, session: AcmeSession) -> None:
        self.session = session

    async def execute(self) -> bool:
        return self.session.execution is current_execution() is not None


class AcmeRoutes(RestInterface[bool]):
    prefix = "/acme"
    routes = (RestRoute(use_case=AcmeRouted, method="GET", path="/"),)


class TestAdaptersBuildInsideTheExecution:
    async def test_the_invoker_builds_inside_the_execution(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        invoker = AppInvoker(factory, executor, cast(UseCaseRegistry, None), identity=ALICE)

        probe = await invoker.invoke(AcmeProbe)

        assert probe.session.caller is ALICE

    async def test_the_inline_job_service_builds_inside_the_execution(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        job = await InlineJobService(factory, executor).run(AcmeProbeJob)

        assert job.session.caller is ANONYMOUS

    async def test_an_inline_dispatch_builds_inside_the_execution(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        results: list[Any] = []
        dispatch = _PendingDispatch(
            job_type=AcmeProbeJob,
            task_id="acme-task",
            payload={},
            params=None,
            on_success=None,
            on_failure=None,
            result_holder=results,
            factory=factory,
            executor=executor,
            identity=ALICE,
        )

        await dispatch.run()

        assert results[0].session.caller is ALICE

    async def test_the_job_runner_builds_inside_the_execution(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        envelope = JobEnvelope(job="AcmeProbeJob", identity=encode_identity(ALICE))

        job = await run_job(
            envelope,
            AcmeProbeJob,
            factory=factory,
            executor=executor,
            observability=ObservabilityRuntime.noop(),
            message_id=None,
        )

        assert job.session.caller == ALICE

    async def test_the_celery_job_service_builds_inside_the_execution(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        service = CeleryJobService(cast(Any, None), factory=factory, executor=executor)

        job = await service.run(AcmeProbeJob)

        assert job.session.caller is ANONYMOUS

    def test_a_rest_route_builds_inside_the_execution(self, factory: UseCaseFactory) -> None:
        compiler = UseCaseCompiler()
        compiler.compile(AcmeRouted)
        factory.register(AcmeRouted)
        app = FastAPI()
        routes = RestInterfaceCompiler(compiler).compile(AcmeRoutes)
        bind_interfaces(
            app, routes, factory, RuntimeExecutor(compiler), ObservabilityRuntime.noop()
        )

        response = TestClient(app).get("/acme/")

        assert response.status_code == 200
        assert response.json() is True
