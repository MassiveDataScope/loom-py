"""Each execution sees its own identity and cache, across tasks and nesting.

What a task holding a copy of an execution that has ended reads is in
``test_lifetime.py``.
"""

from __future__ import annotations

import asyncio

import pytest

from loom.core.errors import Unauthenticated
from loom.core.execution import ExecutionScope, current_execution, enter, execution_identity, leave
from loom.core.identity import Identity, current_caller
from tests.unit.core.execution.context.doubles import ACME_ALICE, ACME_BOB


class TestInterleavedExecutions:
    async def test_two_interleaved_tasks_each_keep_their_own_identity_and_cache(self) -> None:
        """Both tasks are inside their executions at once, switching at every step."""
        alice_inside, bob_inside = asyncio.Event(), asyncio.Event()

        async def run(
            identity: Identity, mine: asyncio.Event, theirs: asyncio.Event
        ) -> list[object]:
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
            run(ACME_ALICE, alice_inside, bob_inside), run(ACME_BOB, bob_inside, alice_inside)
        )

        assert alice == [ACME_ALICE, "acme-alice"] * 3
        assert bob == [ACME_BOB, "acme-bob"] * 3
        assert current_execution() is None


class TestEnter:
    def test_enter_installs_the_identity_given_without_inheriting(self) -> None:
        """Inheritance belongs to the executor: ``enter(None, ...)`` inside Alice has no caller."""
        with ExecutionScope(ACME_ALICE):
            token = enter(None)
            try:
                assert current_execution() is not None
                assert current_caller() is None
            finally:
                leave(token)
            assert execution_identity() is ACME_ALICE

    def test_enter_none_outside_any_execution_is_still_an_execution(self) -> None:
        """An execution with no caller is still an execution: reading its caller fails closed."""
        token = enter(None)
        try:
            with pytest.raises(Unauthenticated):
                execution_identity()
        finally:
            leave(token)
        assert current_execution() is None


class TestExecutionContext:
    def test_it_holds_resources_and_no_identity(self) -> None:
        """Who acts is the acting identity ``enter`` fixed; the context keeps only resources."""
        with ExecutionScope(ACME_ALICE):
            context = current_execution()
            assert context is not None
            assert not hasattr(context, "identity")
            with pytest.raises(AttributeError):
                context.identity = ACME_BOB  # type: ignore[attr-defined]
            assert execution_identity() is ACME_ALICE

    def test_a_nested_execution_starts_with_a_fresh_cache(self) -> None:
        """L12: the outer execution and a nested one share no ``REQUEST`` instance."""
        with ExecutionScope(ACME_ALICE):
            outer = current_execution()
            assert outer is not None
            outer.store("acme-client", "outer")
            with ExecutionScope(ACME_ALICE):
                inner = current_execution()
                assert inner is not None
                assert inner is not outer
                assert inner.get("acme-client") is None
            assert current_execution() is outer
            assert outer.get("acme-client") == "outer"

    def test_the_cache_is_allocated_on_the_first_store(self) -> None:
        """An execution that stores nothing allocates no dict; a stored value is returned."""
        with ExecutionScope(ACME_ALICE):
            context = current_execution()
            assert context is not None
            assert context._cache is None  # pyright: ignore[reportPrivateUsage]
            missing = object()
            assert context.get("acme-client", missing) is missing
            context.store("acme-client", None)
            context.store("acme-ledger", 1)
            assert context.get("acme-client", missing) is None
            assert context.get("acme-ledger") == 1
