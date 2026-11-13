"""Each execution sees its own identity and cache, across tasks and nesting."""

from __future__ import annotations

import asyncio

import pytest

from loom.core.execution import ExecutionScope, current_execution, enter, execution_identity, leave
from loom.core.identity import Identity

_ALICE = Identity(subject="acme-alice", mechanism="test")
_BOB = Identity(subject="acme-bob", mechanism="test")


async def test_two_interleaved_tasks_each_keep_their_own_identity_and_cache() -> None:
    """Both tasks are inside their executions at once, switching at every step."""
    alice_inside, bob_inside = asyncio.Event(), asyncio.Event()

    async def run(identity: Identity, mine: asyncio.Event, theirs: asyncio.Event) -> list[object]:
        token = enter(identity)
        try:
            context = current_execution()
            assert context is not None
            context.store("acme-report", identity.subject)
            mine.set()
            await theirs.wait()
            seen: list[object] = []
            for _ in range(3):
                await asyncio.sleep(0)
                seen += [execution_identity(), context.get("acme-report")]
            return seen
        finally:
            leave(token)

    alice, bob = await asyncio.gather(
        run(_ALICE, alice_inside, bob_inside), run(_BOB, bob_inside, alice_inside)
    )

    assert alice == [_ALICE, "acme-alice"] * 3
    assert bob == [_BOB, "acme-bob"] * 3
    assert current_execution() is None


def test_enter_installs_the_identity_given_without_inheriting() -> None:
    """NS2: inheritance belongs to the executor; ``enter(None)`` inside Alice has no caller."""
    with ExecutionScope(_ALICE):
        token = enter(None)
        try:
            context = current_execution()
            assert context is not None
            assert context.identity is None
        finally:
            leave(token)
        assert execution_identity() is _ALICE


def test_the_identity_of_an_execution_is_read_only() -> None:
    """Every task copied from the execution shares the object; nobody may rewrite it."""
    with ExecutionScope(_ALICE):
        context = current_execution()
        assert context is not None
        with pytest.raises(AttributeError):
            context.identity = _BOB  # type: ignore[misc]
        assert execution_identity() is _ALICE


def test_a_nested_execution_starts_with_a_fresh_cache() -> None:
    """L12: the outer execution and a nested one share no ``REQUEST`` instance."""
    with ExecutionScope(_ALICE):
        outer = current_execution()
        assert outer is not None
        outer.store("acme-client", "outer")
        with ExecutionScope(_ALICE):
            inner = current_execution()
            assert inner is not None
            assert inner is not outer
            assert inner.get("acme-client") is None
        assert current_execution() is outer
        assert outer.get("acme-client") == "outer"


def test_the_cache_is_allocated_on_the_first_store() -> None:
    """An execution that stores nothing allocates no dict; a stored value is returned."""
    with ExecutionScope(_ALICE):
        context = current_execution()
        assert context is not None
        assert context._cache is None  # pyright: ignore[reportPrivateUsage]
        missing = object()
        assert context.get("acme-client", missing) is missing
        context.store("acme-client", None)
        context.store("acme-ledger", 1)
        assert context.get("acme-client", missing) is None
        assert context.get("acme-ledger") == 1


async def test_a_task_outliving_its_execution_keeps_identity_and_cache() -> None:
    """D2: a task copies the context, so it keeps both after the execution ends."""
    release = asyncio.Event()

    async def background() -> tuple[Identity, object]:
        await release.wait()
        context = current_execution()
        assert context is not None
        return execution_identity(), context.get("acme-client")

    with ExecutionScope(_ALICE):
        context = current_execution()
        assert context is not None
        context.store("acme-client", "shared")
        task = asyncio.create_task(background())

    assert current_execution() is None
    release.set()
    assert await task == (_ALICE, "shared")
