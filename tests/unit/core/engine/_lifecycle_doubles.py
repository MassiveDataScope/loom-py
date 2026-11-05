"""Doubles shared by the executor lifecycle tests (audit regressions and contract suite)."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, ClassVar

import pytest

from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import active_channel
from loom.core.job.context import add_pending_dispatch
from loom.core.repository.sqlalchemy.transactional import _mutations
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.scope import current_scope
from loom.core.use_case.use_case import UseCase


class Log:
    """Ordered entries appended by every double of one scenario."""

    def __init__(self) -> None:
        self.entries: list[str] = []

    def __call__(self, entry: str) -> None:
        self.entries.append(entry)

    def index(self, entry: str) -> int:
        return self.entries.index(entry)


class Metrics:
    """Metrics adapter recording every event, in order, on the shared log when given."""

    def __init__(self, log: Log | None = None) -> None:
        self.events: list[RuntimeEvent] = []
        self._log = log

    def on_event(self, event: RuntimeEvent) -> None:
        self.events.append(event)
        if self._log is not None:
            self._log(f"event.{event.kind.name}")

    def kinds(self) -> list[EventKind]:
        return [event.kind for event in self.events]

    def only(self, kind: EventKind) -> RuntimeEvent:
        matching = [event for event in self.events if event.kind is kind]
        assert len(matching) == 1, matching
        return matching[0]


class Broker:
    """Job service double: ``dispatch`` enqueues a send on the post-commit channel."""

    def __init__(self, log: Log) -> None:
        self._log = log
        self.sent: list[str] = []
        self.failing: set[str] = set()

    def dispatch(self, job_name: str) -> None:
        def send() -> None:
            scope = current_scope()
            open_uow = scope is not None and bool(scope.participants)
            self._log(f"broker.send({job_name}) uow={open_uow}")
            if job_name in self.failing:
                raise ConnectionError("broker down")
            self.sent.append(job_name)

        add_pending_dispatch(send)
        self._log(f"dispatch.queued({job_name})")


class StubUnitOfWork:
    """Unit of work following the adapter contract: a commit failure rolls back.

    Stands in for a real adapter where the scenario is about the executor,
    not about driver behaviour; the contract suite covers the adapters.

    Args:
        log: Shared log every step is appended to.
        commit_raises: Exception ``commit`` raises, if any.
        begin_raises: Exception ``begin`` raises, if any.
    """

    transactional: ClassVar[bool] = True

    def __init__(
        self,
        log: Log,
        *,
        commit_raises: Exception | None = None,
        begin_raises: Exception | None = None,
    ) -> None:
        self._log = log
        self._commit_raises = commit_raises
        self._begin_raises = begin_raises

    async def begin(self) -> None:
        self._log("uow.begin")
        if self._begin_raises is not None:
            raise self._begin_raises

    async def commit(self) -> None:
        self._log("uow.commit")
        if self._commit_raises is not None:
            raise self._commit_raises

    async def rollback(self) -> None:
        self._log("uow.rollback")

    async def __aenter__(self) -> StubUnitOfWork:
        await self.begin()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            if exc_type is None:
                await self._commit_or_rollback()
            else:
                await self.rollback()
        finally:
            self._log("uow.closed")

    async def _commit_or_rollback(self) -> None:
        try:
            await self.commit()
        except Exception:
            await self.rollback()
            raise


class StubUnitOfWorkFactory:
    """Factory handing out :class:`StubUnitOfWork` instances and recording them.

    Args:
        log: Shared log passed to every created unit of work.
        options: Keyword arguments forwarded to :class:`StubUnitOfWork`.
    """

    def __init__(self, log: Log, **options: Any) -> None:
        self._log = log
        self._options = options
        self.created: list[StubUnitOfWork] = []

    def create(self) -> UnitOfWork:
        uow = StubUnitOfWork(self._log, **self._options)
        self.created.append(uow)
        return uow


Body = Callable[[], Awaitable[Any]]


class BusinessError(Exception):
    """A business failure a body raises."""


class Run(UseCase[Any, Any]):
    """Runs whatever body the test hands it."""

    def __init__(self, body: Body) -> None:
        self._body = body

    async def execute(self) -> Any:
        return await self._body()


class Ok(UseCase[Any, str]):
    async def execute(self, value: str) -> str:
        return value


class Boom(UseCase[Any, str]):
    async def execute(self, value: str) -> str:
        raise RuntimeError("boom")


class Hang(UseCase[Any, str]):
    async def execute(self, value: str) -> str:
        await asyncio.Event().wait()
        return value


class Dispatching(UseCase[Any, str]):
    def __init__(self, broker: Broker) -> None:
        self._broker = broker

    async def execute(self, value: str) -> str:
        self._broker.dispatch(value)
        return value


class DispatchThenFail(UseCase[Any, str]):
    def __init__(self, broker: Broker) -> None:
        self._broker = broker

    async def execute(self, value: str) -> str:
        self._broker.dispatch(value)
        raise RuntimeError("after dispatch")


def context_is_clean() -> bool:
    """Whether no invocation scope, channel or mutation list stays bound."""
    return current_scope() is None and active_channel() is None and _mutations.get() is None


def start_hanging(executor: RuntimeExecutor) -> tuple[asyncio.Task[None], list[bool]]:
    """Start a hanging execution in a task that records its own context state at the end.

    The task runs in a copied context, so the cleanliness check must happen
    there: the caller's context would look clean even if the adapter leaked.
    """
    observed: list[bool] = []

    async def run() -> None:
        try:
            await executor.execute(Hang(), params={"value": "x"})
        finally:
            observed.append(context_is_clean())

    return asyncio.create_task(run()), observed


async def cancel_mid_flight(executor: RuntimeExecutor) -> None:
    """Cancel a hanging execution once and assert the task saw a clean context."""
    task, observed = start_hanging(executor)
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert observed == [True]
