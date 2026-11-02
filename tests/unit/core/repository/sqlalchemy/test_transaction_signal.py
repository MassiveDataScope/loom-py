"""``@transactional`` drives ``loom.core.transaction``'s atomic-transaction signal.

The signal answers exactly one question for a caller elsewhere in the stack
(the cache bump deferral): is a transaction open that will commit later? The
unit of work the decorator runs in opens it around the whole body and closes
it once the session is torn down; a nested call that joins it must not open a
second one.
"""

from __future__ import annotations

import importlib

import pytest

from loom.core.repository.sqlalchemy.transactional import transactional
from loom.core.transaction import in_atomic_transaction

from .conftest import MockSessionManager

uow_module = importlib.import_module("loom.core.repository.sqlalchemy.uow")


class _Owner:
    """Owns its own transaction; records the signal seen inside the body."""

    def __init__(self, session_manager: MockSessionManager, observed: list[bool]) -> None:
        self.session_manager = session_manager
        self.observed = observed

    @transactional
    async def execute(self) -> str:
        self.observed.append(in_atomic_transaction())
        return "ok"


class _OuterOwner:
    """Delegates to an inner ``@transactional`` method on the same session."""

    def __init__(self, session_manager: MockSessionManager, observed: list[bool]) -> None:
        self.session_manager = session_manager
        self.observed = observed
        self.inner = _Owner(session_manager, observed)

    @transactional
    async def execute(self) -> str:
        self.observed.append(in_atomic_transaction())
        return await self.inner.execute()


async def test_signal_is_closed_before_the_owned_session_opens(
    mock_session_manager: MockSessionManager,
) -> None:
    assert in_atomic_transaction() is False
    owner = _Owner(mock_session_manager, observed=[])
    await owner.execute()
    assert in_atomic_transaction() is False


async def test_signal_is_open_for_the_duration_of_the_owned_body(
    mock_session_manager: MockSessionManager,
) -> None:
    observed: list[bool] = []
    owner = _Owner(mock_session_manager, observed)

    await owner.execute()

    assert observed == [True]


async def test_signal_closes_after_a_rollback(
    mock_session_manager: MockSessionManager,
) -> None:
    class _FailingOwner(_Owner):
        @transactional
        async def execute(self) -> str:
            self.observed.append(in_atomic_transaction())
            raise RuntimeError("boom")

    owner = _FailingOwner(mock_session_manager, observed=[])

    with pytest.raises(RuntimeError, match="boom"):
        await owner.execute()

    assert owner.observed == [True]
    assert in_atomic_transaction() is False


async def test_nested_call_sees_the_signal_but_opens_no_second_one(
    mock_session_manager: MockSessionManager,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[bool] = []
    outer = _OuterOwner(mock_session_manager, observed)
    real_open = uow_module.open_atomic_transaction
    calls: list[None] = []

    def _counting_open() -> object:
        calls.append(None)
        return real_open()

    monkeypatch.setattr(uow_module, "open_atomic_transaction", _counting_open)

    result = await outer.execute()

    assert result == "ok"
    # Both the outer body and the inner, nested body ran with the signal open.
    assert observed == [True, True]
    assert len(calls) == 1
    assert in_atomic_transaction() is False
