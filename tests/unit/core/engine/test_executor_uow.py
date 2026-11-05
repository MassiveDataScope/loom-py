"""Executor unit-of-work shapes the adapter contract suite cannot express.

``tests/unit/core/engine/contract`` runs every lifecycle scenario against the
real adapters, always with a ``uow_factory`` and with ``read_only`` passed at
the call site.  Two shapes stay here:

- an executor built **without** a factory at all;
- a use case declaring ``read_only`` on the class, so the plan carries it.

FR-015: the pins on ``flush_pending_dispatches`` / ``clear_pending_dispatches``
being called by the executor were replaced by observed dispatches, since the
executor now drains its own post-commit channel and no longer imports the job
context helpers.  The executor-level double-cancellation test was removed: the
cancellation shield moved into the adapters (lead decision 11), and the
behaviour is pinned by
``tests/unit/core/uow/test_sqlalchemy_uow.py::test_second_cancellation_during_rollback_still_rolls_back_and_closes``
and
``tests/unit/core/repository/mongo/test_uow.py::TestTransactional::test_second_cancellation_during_abort_still_aborts_and_ends``.
"""

from __future__ import annotations

from typing import Any

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.job.context import add_pending_dispatch
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.uow.coordinator import OrderedCommits
from loom.core.use_case.use_case import UseCase

from ._lifecycle_doubles import Log, StubUnitOfWorkFactory


async def test_dispatch_runs_at_the_end_without_uow_factory() -> None:
    """FR-003: with no factory configured the dispatches still run, once, at the end."""
    executor = RuntimeExecutor(UseCaseCompiler())
    ran: list[str] = []

    class _DispatchNoUoW(UseCase[Any, str]):
        async def execute(self) -> str:
            add_pending_dispatch(lambda: ran.append("ran"))
            ran.append("executed")
            return "ok"

    await executor.execute(_DispatchNoUoW())

    assert ran == ["executed", "ran"]


async def test_read_only_declared_on_the_class_skips_the_unit_of_work() -> None:
    """``UseCase.read_only = True`` reaches the executor through the compiled plan."""

    class _ReadOnlyUseCase(UseCase[Any, str]):
        read_only = True

        async def execute(self) -> str:
            return "read"

    log = Log()
    factory = StubUnitOfWorkFactory(log)
    executor = RuntimeExecutor(
        UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: factory})
    )

    assert await executor.execute(_ReadOnlyUseCase()) == "read"
    assert factory.created == []
    assert log.entries == []
