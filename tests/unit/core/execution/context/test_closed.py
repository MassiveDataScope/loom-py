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
from loom.core.identity import ANONYMOUS, Identity, current_identity, reset_identity, set_identity

_ALICE = Identity(subject="acme-alice", mechanism="test")


class _Boom(Exception):
    pass


def test_no_execution_raises_execution_required_even_with_an_ambient_identity() -> None:
    """D2: the ambient ``set_identity`` is never a fallback for the execution identity."""
    token = set_identity(_ALICE)
    try:
        with pytest.raises(ExecutionRequired):
            execution_identity()
    finally:
        reset_identity(token)


def test_the_anonymous_identity_is_carried_like_any_other() -> None:
    """Refusing ``ANONYMOUS`` is the consumer's rule, not the context's."""
    with ExecutionScope(ANONYMOUS):
        assert execution_identity() is ANONYMOUS


def test_no_execution_hides_the_enclosing_execution_and_restores_it() -> None:
    """Singletons are built here: an identity read in a constructor fails closed."""
    with ExecutionScope(_ALICE):
        outer = current_execution()
        with NoExecution():
            assert current_execution() is None
            with pytest.raises(ExecutionRequired):
                execution_identity()
        assert current_execution() is outer


def test_a_raising_body_closes_the_execution() -> None:
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


async def test_a_cancelled_body_closes_the_execution() -> None:
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


def test_an_unresolved_marker_is_an_attribute_error() -> None:
    """S4; T07 owns the marker behaviour."""
    assert issubclass(MarkerNotResolved, AttributeError)
