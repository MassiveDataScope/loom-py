"""T120: a read-only execution writes nothing, through a dispatched job neither."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom.celery.service import CeleryJobService
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.service import InlineJobService, JobService
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import Frame, InvocationScope, UndeclaredStoreWrite, invocation
from loom.testing import RecordingJobService
from tests.helpers.jobs import EmailJob, SQSClient, sqs_service
from tests.unit.core.uow._coordinator_fakes import Factory, Ledger, PhasedStore

pytestmark = pytest.mark.timeout(10)

_Sent = Callable[[], int]


def _inline() -> tuple[JobService, _Sent]:
    executor = AsyncMock()
    return InlineJobService(MagicMock(), executor), lambda: executor.execute.await_count


def _celery() -> tuple[JobService, _Sent]:
    app = MagicMock()
    return CeleryJobService(app), lambda: app.send_task.call_count


def _sqs() -> tuple[JobService, _Sent]:
    client = SQSClient()
    return sqs_service(client), lambda: len(client.requests)


def _recording() -> tuple[JobService, _Sent]:
    service = RecordingJobService()
    return service, lambda: len(service.dispatched)


_SERVICES = {"inline": _inline, "celery": _celery, "sqs": _sqs, "recording": _recording}


@asynccontextmanager
async def _read_only_invocation() -> AsyncIterator[InvocationScope]:
    async with (
        invocation(OrderedCommits({}), read_only=True) as scope,
        scope.frame(Frame(None)),
    ):
        yield scope


@asynccontextmanager
async def _read_only_frame_in_a_writer() -> AsyncIterator[InvocationScope]:
    store = PhasedStore(DEFAULT_STORE, Ledger())
    async with (
        invocation(OrderedCommits({DEFAULT_STORE: Factory(store)})) as scope,
        scope.frame(Frame(DEFAULT_STORE)),
        scope.frame(Frame(None)),
    ):
        yield scope


_READ_ONLY: dict[str, Callable[[], AbstractAsyncContextManager[InvocationScope]]] = {
    "read-only-invocation": _read_only_invocation,
    "read-only-frame-in-a-writer": _read_only_frame_in_a_writer,
}


@pytest.fixture(params=sorted(_SERVICES))
def service(request: pytest.FixtureRequest) -> Iterator[tuple[JobService, _Sent]]:
    yield _SERVICES[request.param]()
    clear_pending_dispatches()


@pytest.mark.parametrize("read_only", sorted(_READ_ONLY))
async def test_a_dispatch_from_a_read_only_execution_is_refused_and_never_sent(
    service: tuple[JobService, _Sent], read_only: str
) -> None:
    jobs, sent = service
    async with _READ_ONLY[read_only]():
        with pytest.raises(UndeclaredStoreWrite, match="a job dispatch is refused"):
            jobs.dispatch(EmailJob, payload={"to": "a@b.c"})

    await flush_pending_dispatches()
    assert sent() == 0


async def test_a_dispatch_outside_a_read_only_execution_is_sent(
    service: tuple[JobService, _Sent],
) -> None:
    jobs, sent = service
    jobs.dispatch(EmailJob, payload={"to": "a@b.c"})

    await flush_pending_dispatches()
    assert sent() == 1
