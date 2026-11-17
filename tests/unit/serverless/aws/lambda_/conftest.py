from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path

import pytest

from loom.composition import AppConfig, Application, Project
from loom.composition.resources import Resource, ResourceKind
from loom.core.config import ConfigContext
from loom.core.errors import PostCommitError, StoresNotCommitted
from loom.core.identity import current_identity
from loom.core.job.job import Job
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.uow.coordinator import InvocationOutcome, Uncertain
from loom.core.stores.uow.scope import invocation_error
from loom.core.tracing import get_trace_id
from loom.serverless.aws.lambda_ import LambdaAdapter, LambdaHandler
from tests.helpers.composition import composition_project
from tests.helpers.extras import patched_aws_clients
from tests.helpers.loops import event_loops
from tests.helpers.sqs import FakeSqs, queue_url
from tests.unit.serverless.aws.lambda_._probe import Probe
from tests.unit.serverless.aws.lambda_._records import (
    WORKER,
    RecordFactory,
    make_record,
    with_worker,
)

__all__ = ["event_loops"]


@pytest.fixture
def sqs() -> FakeSqs:
    """The SQS client every function of the test consumes and settles through."""
    return FakeSqs()


@pytest.fixture
def no_aws_clients(sqs: FakeSqs) -> Iterator[None]:
    """A consumer of a queue builds its SQS client at Init; here it is the fake, never AWS."""
    with patched_aws_clients(sqs):
        yield


@pytest.fixture
def probe() -> tuple[type[Job[None]], Probe]:
    state = Probe()

    class ProbeJob(Job[None]):
        def execute(self, marker: str) -> None:
            state.seen.append((marker, current_identity().subject, get_trace_id()))
            state.loops.append(asyncio.get_running_loop())
            if marker == "interrupt":
                raise KeyboardInterrupt
            if marker == "cancel":
                raise asyncio.CancelledError
            if marker == "fail":
                raise ValueError("job failed")
            if marker == "committed":
                raise PostCommitError(committed=True, failures=(ValueError("post commit"),))
            if marker == "scope":
                raise StoresNotCommitted("partially committed")
            if marker == "uncertain":
                outcome = InvocationOutcome((Uncertain(DEFAULT_STORE, OSError("ack lost")),))
                raise invocation_error(outcome, None) or AssertionError(outcome)

    return ProbeJob, state


@pytest.fixture
def seen(probe: tuple[type[Job[None]], Probe]) -> list[tuple[str, str | None, str | None]]:
    return probe[1].seen


@pytest.fixture
def loops(probe: tuple[type[Job[None]], Probe]) -> list[asyncio.AbstractEventLoop]:
    return probe[1].loops


@pytest.fixture
def project(tmp_path: Path, no_aws_clients: None) -> Project:
    document = with_worker({"app": {"name": "lambda-sqs"}}, batch_size=5)
    document["sqs"]["queues"]["jobs_fifo"] = {
        "url": f"{queue_url('jobs')}.fifo",
        "dead_letter": "discard",
    }
    return composition_project(
        ConfigContext.from_dict(document),
        app=AppConfig(name="lambda-sqs", entry=WORKER),
        code_path=tmp_path,
        config_root=tmp_path,
    )


@pytest.fixture
def built(
    project: Project, probe: tuple[type[Job[None]], Probe]
) -> tuple[LambdaAdapter, Application, list[str]]:
    adapter = LambdaAdapter(jobs=[probe[0]])
    lifecycle: list[str] = []

    @asynccontextmanager
    async def opened() -> AsyncIterator[None]:
        lifecycle.append("open")
        yield
        lifecycle.append("close")

    application = project.compose(adapter)
    application.resources = (
        *application.resources,
        Resource(kind=ResourceKind.STORE, name="probe", manager=opened),
    )
    return adapter, application, lifecycle


@pytest.fixture
def handler(built: tuple[LambdaAdapter, Application, list[str]]) -> Iterator[LambdaHandler]:
    adapter, application, _ = built
    yield adapter.bind(application)
    adapter.close()


@pytest.fixture
def record(probe: tuple[type[Job[None]], Probe]) -> RecordFactory:
    return partial(make_record, job_name=probe[0].__qualname__)
