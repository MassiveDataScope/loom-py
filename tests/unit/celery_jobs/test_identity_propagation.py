"""The caller identity survives the broker hop, or the job fails closed.

A context variable does not cross a process boundary, so the identity travels
inside the job envelope. These tests pin both ends of that wire contract: what
a dispatch puts in, what the worker takes out, and what happens to an envelope
minted before the contract existed.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from typing import Any, ClassVar

import pytest

from loom.celery.service import CeleryJobTransport
from loom.core.errors.auth import MissingCaller
from loom.core.identity import ANONYMOUS, Identity, current_identity, reset_identity, set_identity
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.logger.failure import FailureReference, exception_type
from loom.core.use_case.markers import Caller
from tests.helpers.celery import CeleryBroker
from tests.helpers.jobs import CeleryWorker

CALLER = Identity(
    subject="user-1", roles=("role_a",), attributes={"email": "ada@example.com"}, mechanism="jwt"
)


class ReportJob(Job[str]):
    def execute(self, caller: Identity = Caller()) -> str:
        return caller.subject


class ObservedJob(Job[None]):
    seen: ClassVar[list[Identity]] = []

    def execute(self) -> None:
        self.seen.append(current_identity())


@pytest.fixture(autouse=True)
def _forget_who_was_seen() -> None:
    ObservedJob.seen.clear()


@pytest.fixture
def worker() -> Iterator[CeleryWorker]:
    worker = CeleryWorker()
    yield worker
    worker.close()


@pytest.fixture
def dispatched_identity() -> Callable[[Identity | None], Awaitable[dict[str, Any] | None]]:
    """Dispatch a job as an identity; return the identity its envelope carries."""

    async def dispatch(identity: Identity | None) -> dict[str, Any] | None:
        broker = CeleryBroker([ReportJob.__queue__])
        token = set_identity(identity) if identity is not None else None
        try:
            JobService(CeleryJobTransport(broker.app)).dispatch_after_commit(ReportJob)
            await flush_pending_dispatches()
        finally:
            if token is not None:
                reset_identity(token)
        (message,) = broker.sent()
        carried: dict[str, Any] | None = message.kwargs["identity"]
        return carried

    return dispatch


class TestDispatchSide:
    async def test_dispatch_puts_the_caller_in_the_envelope_with_the_deadline_of_its_delegation(
        self, dispatched_identity: Callable[[Identity | None], Awaitable[dict[str, Any] | None]]
    ) -> None:
        carried = await dispatched_identity(CALLER)

        assert carried is not None
        assert isinstance(carried.pop("until"), float)
        assert carried == {
            "subject": "user-1",
            "roles": ["role_a"],
            "attributes": {"email": "ada@example.com"},
            "mechanism": "jwt",
        }

    @pytest.mark.parametrize("identity", [None, ANONYMOUS], ids=["no caller", "anonymous"])
    async def test_a_dispatch_without_an_authenticated_caller_sends_no_identity(
        self,
        dispatched_identity: Callable[[Identity | None], Awaitable[dict[str, Any] | None]],
        identity: Identity | None,
    ) -> None:
        assert await dispatched_identity(identity) is None


class TestWorkerSide:
    def test_the_job_runs_as_the_caller_of_the_envelope(self, worker: CeleryWorker) -> None:
        assert worker.run(ReportJob, identity={"subject": "user-1"}) == "user-1"

    def test_the_worker_publishes_the_caller_in_the_context_for_further_dispatches(
        self, worker: CeleryWorker
    ) -> None:
        worker.run(ObservedJob, identity={"subject": "user-1"})

        assert [caller.subject for caller in ObservedJob.seen] == ["user-1"]

    def test_the_worker_restores_the_previous_context_afterwards(
        self, worker: CeleryWorker
    ) -> None:
        worker.run(ObservedJob, identity={"subject": "user-1"})

        assert current_identity() is ANONYMOUS

    def test_an_envelope_without_identity_runs_the_job_as_no_one(
        self, worker: CeleryWorker
    ) -> None:
        worker.run(ObservedJob)

        assert [caller.is_authenticated for caller in ObservedJob.seen] == [False]

    def test_a_job_declaring_a_caller_fails_closed_on_an_envelope_without_identity(
        self, worker: CeleryWorker
    ) -> None:
        with pytest.raises(FailureReference) as raised:
            worker.run(ReportJob, attempt=3)

        assert (raised.value.exception_type, raised.value.expected) == (
            exception_type(MissingCaller),
            True,
        )
