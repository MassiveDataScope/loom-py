"""Regression test for the external audit finding F01, over a session-manager oracle.

The executor's coordinator drives the SQLAlchemy unit of work phase by phase
(begin, flush, commit or rollback, close), so the session context exits
exactly once per execution and nothing stays bound to the context
afterwards.  F02, F07 and the nesting rules moved to
``tests/unit/core/engine/contract``, which runs them against every real
adapter; the map is at the bottom of this file.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy.engine import make_url

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.coordinator import OrderedCommits

from ._lifecycle_doubles import Boom, Log, Ok, cancel_mid_flight, context_is_clean

# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


class _FakeSession:
    def __init__(self, log: Log) -> None:
        self._log = log

    async def flush(self) -> None:
        self._log("session.flush")

    async def commit(self) -> None:
        self._log("session.commit")

    async def rollback(self) -> None:
        self._log("session.rollback")


class _FakeSessionCM:
    def __init__(self, log: Log) -> None:
        self._log = log
        self.session = _FakeSession(log)
        self.exits = 0

    async def __aenter__(self) -> _FakeSession:
        self._log("session_cm.enter")
        return self.session

    async def __aexit__(self, *args: object) -> None:
        self._log("session_cm.exit")
        self.exits += 1


class _FakeSessionManager:
    store = DEFAULT_STORE

    def __init__(self, log: Log) -> None:
        self._log = log
        self.cms: list[_FakeSessionCM] = []
        self.engine = SimpleNamespace(url=make_url("sqlite+aiosqlite://"))

    def session(self) -> _FakeSessionCM:
        cm = _FakeSessionCM(self._log)
        self.cms.append(cm)
        return cm


def _executor(factory: SQLAlchemyUnitOfWorkFactory) -> RuntimeExecutor:
    return RuntimeExecutor(UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: factory}))


# ---------------------------------------------------------------------------
# F01 — the SQLAlchemy session context exits exactly once and nothing leaks
# ---------------------------------------------------------------------------


class TestF01SessionClosed:
    def _sqlalchemy_executor(self, log: Log) -> tuple[RuntimeExecutor, _FakeSessionManager]:
        manager = _FakeSessionManager(log)
        # The fake stands in for the real SessionManager.
        factory = SQLAlchemyUnitOfWorkFactory(manager)  # type: ignore[arg-type]
        return _executor(factory), manager

    async def test_commit_path_exits_the_session_once_and_unbinds_everything(self) -> None:
        log = Log()
        executor, manager = self._sqlalchemy_executor(log)

        await executor.execute(Ok(), params={"value": "x"})

        assert [cm.exits for cm in manager.cms] == [1]
        assert log.entries == [
            "session_cm.enter",
            "session.flush",
            "session.commit",
            "session_cm.exit",
        ]
        assert context_is_clean()

    async def test_rollback_path_exits_the_session_once_and_unbinds_everything(self) -> None:
        log = Log()
        executor, manager = self._sqlalchemy_executor(log)

        action = Boom()
        with pytest.raises(RuntimeError, match="boom"):
            await executor.execute(action, params={"value": "x"})

        assert [cm.exits for cm in manager.cms] == [1]
        assert log.entries == ["session_cm.enter", "session.rollback", "session_cm.exit"]
        assert context_is_clean()

    async def test_cancellation_path_exits_the_session_once_and_unbinds_everything(
        self,
    ) -> None:
        log = Log()
        executor, manager = self._sqlalchemy_executor(log)

        await cancel_mid_flight(executor)

        assert [cm.exits for cm in manager.cms] == [1]
        assert log.entries == ["session_cm.enter", "session.rollback", "session_cm.exit"]
        assert context_is_clean()

    async def test_a_later_execution_opens_its_own_session(self) -> None:
        log = Log()
        executor, manager = self._sqlalchemy_executor(log)
        await executor.execute(Ok(), params={"value": "x"})

        await executor.execute(Ok(), params={"value": "y"})

        assert [cm.exits for cm in manager.cms] == [1, 1]
        assert context_is_clean()


# ---------------------------------------------------------------------------
# F02, F07 and nesting live in the contract suite
# ---------------------------------------------------------------------------
#
# They ran here over a stub unit of work.  ``tests/unit/core/engine/contract``
# runs the same scenarios against the real SQLAlchemy, Mongo (transactional
# and no-op) and DynamoDB adapters, so the stub versions were dropped:
#
# F02  dispatch under a unit of work      → contract/test_dispatch.py::
#        test_dispatch_is_sent_after_the_close_and_after_exec_done
#      read-only / no unit of work        → contract/test_dispatch.py::
#        test_read_only_execution_opens_no_unit_of_work_and_sends_at_the_end
#        and test_executor_uow.py::test_dispatch_runs_at_the_end_without_uow_factory
#      failed execution discards          → contract/test_dispatch.py::
#        test_failed_execution_discards_and_the_next_one_sends_nothing_stale
#      broker failure after a commit      → contract/test_dispatch.py::
#        test_broker_failure_is_a_post_commit_error_after_a_committed_transaction
#
# F07  commit failure                     → contract/test_lifecycle.py::
#        TestTerminalEventReflectsTheTransaction::
#        test_commit_failure_is_exec_error_commit_and_the_adapter_closes_once
#      begin failure                      → contract/test_lifecycle.py::
#        TestTerminalEventReflectsTheTransaction::
#        test_begin_failure_emits_exec_start_then_exec_error_begin
#      business failure                   → contract/test_lifecycle.py::
#        TestContextManagerProtocolOnly::
#        test_failure_exits_once_with_the_error_and_unbinds_everything
#      cancellation                       → contract/test_lifecycle.py::
#        TestContextManagerProtocolOnly::
#        test_cancellation_exits_once_and_the_task_sees_a_clean_context
#      EXEC_DONE after the close, timings → contract/test_lifecycle.py::
#        TestContextManagerProtocolOnly::
#        test_success_enters_and_exits_once_through_the_protocol
#      no unit of work → no commit_ms     → contract/test_dispatch.py::
#        test_read_only_execution_opens_no_unit_of_work_and_sends_at_the_end
#
# Nesting  inner joins the outer channel  → contract/test_nesting.py::
#        test_inner_joins_the_outer_unit_of_work_and_channel_one_drain
#      inner owning its unit of work      → contract/test_nesting.py::
#        test_inner_with_its_own_unit_of_work_drains_after_its_close_despite_outer_failure
#      execution from a post-commit action → contract/test_nesting.py::
#        test_an_execution_started_from_a_post_commit_action_opens_its_own_lifecycle
