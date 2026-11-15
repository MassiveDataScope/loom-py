"""A misused token never leaves an identity behind.

Every misuse below would, with a bare ``ContextVar.reset``, either keep the
caller installed or bring back one that already ended. ``ExecutionContext.close`` refuses it
and closes the execution context instead, so later reads fail closed.
"""

from __future__ import annotations

import asyncio

import pytest

from loom.core.execution import (
    ExecutionContext,
    ExecutionRequired,
    ExecutionScope,
    NoExecution,
    current_execution,
    execution_identity,
)
from loom.core.identity import Identity

_ALICE = Identity(subject="acme-alice", mechanism="test")
_BOB = Identity(subject="acme-bob", mechanism="test")


class TestMisusedExecutionTokens:
    def test_leaving_twice_raises_and_closes_the_execution_context(self) -> None:
        outer = ExecutionContext.entered(_ALICE)
        inner = ExecutionContext.entered(_BOB)
        inner.close()

        with pytest.raises(RuntimeError, match="misuse"):
            inner.close()

        assert current_execution() is None
        with pytest.raises(RuntimeError, match="misuse"):
            outer.close()
        assert current_execution() is None

    def test_leaving_out_of_order_never_brings_back_an_ended_caller(self) -> None:
        """``reset(outer)`` then ``reset(inner)`` would reinstall Alice after both ended."""
        outer = ExecutionContext.entered(_ALICE)
        inner = ExecutionContext.entered(_BOB)

        for token in (outer, inner):
            with pytest.raises(RuntimeError, match="misuse"):
                token.close()

        assert current_execution() is None
        with pytest.raises(ExecutionRequired):
            execution_identity()

    def test_leaving_the_enclosing_execution_inside_no_execution_stays_closed(self) -> None:
        """The block does not reinstall Alice, whose token was already misused."""
        token = ExecutionContext.entered(_ALICE)
        with (
            pytest.raises(RuntimeError, match="NoExecution"),
            NoExecution(),
            pytest.raises(RuntimeError, match="misuse"),
        ):
            token.close()
        assert current_execution() is None

        with pytest.raises(RuntimeError, match="misuse"):
            token.close()  # Alice's own ``finally``
        assert current_execution() is None
        with pytest.raises(ExecutionRequired):
            execution_identity()

    def test_an_execution_left_open_inside_no_execution_closes_the_context(self) -> None:
        token = ExecutionContext.entered(_ALICE)
        with pytest.raises(RuntimeError, match="NoExecution"), NoExecution():
            ExecutionContext.entered(_BOB)
        assert current_execution() is None
        with pytest.raises(ExecutionRequired):
            execution_identity()

        with pytest.raises(RuntimeError, match="misuse"):
            token.close()  # Alice's own ``finally`` meets the closed context
        assert current_execution() is None

    async def test_a_token_left_in_another_task_changes_neither_task(self) -> None:
        """The copied context holds the same execution, but the token belongs to its creator."""
        token = ExecutionContext.entered(_ALICE)
        try:
            context = current_execution()

            async def misuse() -> object:
                with pytest.raises(ValueError):
                    token.close()
                return current_execution()

            assert await asyncio.create_task(misuse()) is context
            assert current_execution() is context
        finally:
            token.close()
        assert current_execution() is None

    async def test_a_copied_task_leaving_after_its_parent_fails_closed(self) -> None:
        """The parent's ``close`` retires the token, so a late ``close`` in the copy closes it."""
        token = ExecutionContext.entered(_ALICE)
        task_may_leave = asyncio.Event()

        async def late_leave() -> None:
            await task_may_leave.wait()
            with pytest.raises(RuntimeError, match="misuse"):
                token.close()
            assert current_execution() is None
            with pytest.raises(ExecutionRequired):
                execution_identity()

        task = asyncio.create_task(late_leave())
        token.close()
        task_may_leave.set()
        await task
        assert current_execution() is None

    def test_an_open_scope_refuses_a_second_entry_and_stays_open(self) -> None:
        scope = ExecutionScope(_ALICE)
        with scope:
            with pytest.raises(RuntimeError, match="already open"), scope:
                pass
            assert execution_identity() is _ALICE
        assert current_execution() is None

        with scope:
            assert execution_identity() is _ALICE
        assert current_execution() is None

    def test_an_open_no_execution_refuses_a_second_entry(self) -> None:
        hidden = NoExecution()
        with ExecutionScope(_ALICE):
            with hidden:
                with pytest.raises(RuntimeError, match="already open"), hidden:
                    pass
                assert current_execution() is None
            assert execution_identity() is _ALICE
