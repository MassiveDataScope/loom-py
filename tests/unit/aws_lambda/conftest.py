from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from loom.aws_lambda import LambdaSQSAdapter
from loom.aws_lambda.sqs import SQSHandler
from loom.composition import AppConfig, Application, CompositionSource, compose_application
from loom.composition.resources import Resource, ResourceKind
from loom.core.config import ConfigContext
from loom.core.identity import current_identity
from loom.core.job.job import Job
from loom.core.tracing import get_trace_id
from tests.helpers.composition import composition_source

RecordFactory = Callable[..., dict[str, Any]]


class ProbeJob(Job[None]):
    __retries__ = 0
    seen: list[tuple[str, str | None, str | None]] = []
    loops: list[asyncio.AbstractEventLoop] = []

    def execute(self, marker: str) -> None:
        self.seen.append((marker, current_identity().subject, get_trace_id()))
        self.loops.append(asyncio.get_running_loop())
        if marker == "interrupt":
            raise KeyboardInterrupt
        if marker == "fail":
            raise ValueError("job failed")


@pytest.fixture(autouse=True)
def clean_seen() -> Iterator[None]:
    ProbeJob.seen.clear()
    ProbeJob.loops.clear()
    yield
    ProbeJob.seen.clear()
    ProbeJob.loops.clear()


@pytest.fixture
def seen() -> list[tuple[str, str | None, str | None]]:
    return ProbeJob.seen


@pytest.fixture
def loops() -> list[asyncio.AbstractEventLoop]:
    return ProbeJob.loops


@pytest.fixture
def source(tmp_path: Path) -> CompositionSource:
    return composition_source(
        ConfigContext.from_dict({"app": {"name": "lambda-sqs"}}),
        app=AppConfig(name="lambda-sqs"),
        code_path=tmp_path,
        config_root=tmp_path,
    )


@pytest.fixture
def built(source: CompositionSource) -> tuple[LambdaSQSAdapter, Application, list[str]]:
    adapter = LambdaSQSAdapter(jobs=[ProbeJob])
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
def record() -> RecordFactory:
    def make(
        message_id: str,
        marker: str,
        *,
        identity: dict[str, Any] | None = None,
        trace_id: str | None = None,
        fifo: bool = False,
    ) -> dict[str, Any]:
        return {
            "messageId": message_id,
            "body": json.dumps(
                {
                    "job": "ProbeJob",
                    "payload": {},
                    "params": {"marker": marker},
                    "identity": identity,
                    "trace_id": trace_id,
                }
            ),
            "eventSourceARN": "arn:aws:sqs:us-east-1:123:queue.fifo" if fifo else "queue",
        }

    return make
