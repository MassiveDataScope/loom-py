from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path

import pytest

from loom.aws_lambda import LambdaSQSAdapter
from loom.aws_lambda.sqs import SQSHandler
from loom.composition import AppConfig, Application, CompositionSource, compose_application
from loom.composition.resources import Resource, ResourceKind
from loom.core.config import ConfigContext
from loom.core.engine.post_commit import PostCommitError
from loom.core.identity import current_identity
from loom.core.job.job import Job
from loom.core.tracing import get_trace_id
from loom.core.uow.scope import StoresNotCommitted
from tests.helpers.composition import composition_source
from tests.helpers.loops import event_loops
from tests.unit.aws_lambda._probe import Probe
from tests.unit.aws_lambda._records import RecordFactory, make_record

__all__ = ["event_loops"]


@pytest.fixture
def probe() -> tuple[type[Job[None]], Probe]:
    state = Probe()

    class ProbeJob(Job[None]):
        __retries__ = 0

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

    return ProbeJob, state


@pytest.fixture
def seen(probe: tuple[type[Job[None]], Probe]) -> list[tuple[str, str | None, str | None]]:
    return probe[1].seen


@pytest.fixture
def loops(probe: tuple[type[Job[None]], Probe]) -> list[asyncio.AbstractEventLoop]:
    return probe[1].loops


@pytest.fixture
def source(tmp_path: Path) -> CompositionSource:
    return composition_source(
        ConfigContext.from_dict({"app": {"name": "lambda-sqs"}}),
        app=AppConfig(name="lambda-sqs"),
        code_path=tmp_path,
        config_root=tmp_path,
    )


@pytest.fixture
def built(
    source: CompositionSource, probe: tuple[type[Job[None]], Probe]
) -> tuple[LambdaSQSAdapter, Application, list[str]]:
    adapter = LambdaSQSAdapter(jobs=[probe[0]])
    application = compose_application(source, adapter)
    lifecycle: list[str] = []

    @asynccontextmanager
    async def opened() -> AsyncIterator[None]:
        lifecycle.append("open")
        yield
        lifecycle.append("close")

    application.resources = (
        *application.resources,
        Resource(kind=ResourceKind.STORE, name="probe", manager=opened),
    )
    return adapter, application, lifecycle


@pytest.fixture
def handler(built: tuple[LambdaSQSAdapter, Application, list[str]]) -> Iterator[SQSHandler]:
    adapter, application, _ = built
    yield adapter.bind(application)
    adapter.close()


@pytest.fixture
def record(probe: tuple[type[Job[None]], Probe]) -> RecordFactory:
    return partial(make_record, job_name=probe[0].__qualname__)
