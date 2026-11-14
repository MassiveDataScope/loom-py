"""Reading the identity fails closed, and every exit path closes the execution."""

from __future__ import annotations

import asyncio

import pytest

from loom.core.execution import (
    ExecutionRequired,
    ExecutionScope,
    MarkerNotResolved,
    NoExecution,
    current_execution,
    enter,
    execution_identity,
    leave,
)
from loom.core.identity import (
    ANONYMOUS,
    Identity,
    IdentityFixed,
    current_caller,
    current_identity,
    set_identity,
)
from tests.helpers.delegation import acting_as

_ALICE = Identity(subject="acme-alice", mechanism="test")


class _Boom(Exception):
    pass


class TestExecutionIdentity:
    """Reading the execution identity fails closed outside an execution."""

    def test_no_execution_raises_execution_required_even_with_an_ambient_identity(self) -> None:
        """D2: the ambient ``set_identity`` is never a fallback for the execution identity."""
        with acting_as(_ALICE), pytest.raises(ExecutionRequired):
            execution_identity()

    def test_the_anonymous_identity_is_carried_like_any_other(self) -> None:
        """Refusing ``ANONYMOUS`` is the consumer's rule, not the context's."""
        with ExecutionScope(ANONYMOUS):
            assert execution_identity() is ANONYMOUS


class TestNoExecution:
    """Singletons are built here: no execution, and no caller, fixed."""

    def test_no_execution_hides_the_enclosing_execution_and_restores_it(self) -> None:
        """Singletons are built here: an identity read in a constructor fails closed."""
        with ExecutionScope(_ALICE):
            outer = current_execution()
            with NoExecution():
                assert current_execution() is None
                with pytest.raises(ExecutionRequired):
                    execution_identity()
            assert current_execution() is outer

    def test_no_execution_acts_as_no_one_and_restores_the_caller(self) -> None:
        """D2: a singleton built inside an execution reads ``ANONYMOUS``, never the caller."""
        with ExecutionScope(_ALICE):
            with NoExecution():
                assert current_identity() is ANONYMOUS
                assert current_caller() is None
                with pytest.raises(IdentityFixed):
                    set_identity(_ALICE)
            assert current_caller() is _ALICE


class TestExitPaths:
    """Every exit path closes the execution."""

    def test_a_raising_body_closes_the_execution(self) -> None:
        with pytest.raises(_Boom), ExecutionScope(_ALICE):
            raise _Boom
        assert current_execution() is None

        token = enter(_ALICE)
        with pytest.raises(_Boom):
            try:
                raise _Boom
            finally:
                leave(token)
        assert current_execution() is None

    async def test_a_cancelled_body_closes_the_execution(self) -> None:
        """Cancelled at an await inside the execution, from outside the task."""
        inside = asyncio.Event()
        seen_after: list[object] = []

        async def body() -> None:
            try:
                token = enter(_ALICE)
                try:
                    inside.set()
                    await asyncio.Event().wait()
                finally:
                    leave(token)
            finally:
                seen_after.append(current_execution())

        task = asyncio.create_task(body())
        await inside.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert seen_after == [None]
        assert current_execution() is None
        assert current_identity() is ANONYMOUS


class TestMarkerNotResolved:
    def test_an_unresolved_marker_is_an_attribute_error(self) -> None:
        """S4; T07 owns the marker behaviour."""
        assert issubclass(MarkerNotResolved, AttributeError)
