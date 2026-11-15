"""The Lambda ``http`` route and ``jobs`` envelope build inside the execution.

Each serves through the shared entry points (the router runtime and the job
runner) with the Lambda pieces in front: the Starlette route of a compiled
route, and ``run_envelope`` with the invocation's timeout.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from starlette.applications import Starlette
from starlette.testclient import TestClient

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.identity.wire import encode_identity
from loom.core.job.config import JobSettings
from loom.core.job.envelope import JobEnvelope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case.factory import UseCaseFactory
from loom.rest.compiler import RestInterfaceCompiler
from loom.serverless.aws.lambda_._jobs import run_envelope
from loom.serverless.aws.lambda_.triggers.http import (
    _route,  # pyright: ignore[reportPrivateUsage]
)

from .doubles import ALICE, CALLERS, AcmeProbeJob, AcmeRecordingFactory, AcmeRoutes, as_caller


class TestLambdaHttpRoute:
    @CALLERS
    def test_builds_and_runs_inside_one_execution_as_the_request_caller(
        self,
        factory: UseCaseFactory,
        compiler: UseCaseCompiler,
        caller: Identity | None,
        subject: str,
    ) -> None:
        api = SimpleNamespace(factory=factory, executor=RuntimeExecutor(compiler))
        noop = ObservabilityRuntime.noop()
        [compiled] = RestInterfaceCompiler(compiler).compile(AcmeRoutes)
        app = Starlette(routes=[_route(compiled, cast(Any, api), noop)])

        response = TestClient(as_caller(app, caller)).get("/acme/")  # type: ignore[arg-type]

        assert response.json() == {
            "built_as": subject,
            "runs_as": subject,
            "same_execution": True,
            "ambient": subject,
        }


class TestLambdaJobsEnvelope:
    async def test_builds_inside_an_execution_as_the_envelope_caller(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        built: list[AcmeProbeJob] = []
        kernel = SimpleNamespace(factory=AcmeRecordingFactory(factory, built), executor=executor)
        application = SimpleNamespace(
            kernel=kernel, observability=ObservabilityRuntime.noop(), job_settings=JobSettings()
        )
        envelope = JobEnvelope(job="AcmeProbeJob", identity=encode_identity(ALICE))

        await run_envelope(cast(Any, application), envelope, AcmeProbeJob, None, None)

        [job] = built
        assert job.session.caller == ALICE
        assert job.session.execution is job.ran_in
