"""D4: ``REQUEST`` and ``TRANSACTION`` give one instance per execution, a fresh one outside."""

from __future__ import annotations

import asyncio
import contextvars
import threading
from collections.abc import Callable

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionScope, enter, leave
from loom.core.identity import ANONYMOUS
from tests.unit.core.execution.context.doubles import AcmeBuiltSession

PER_EXECUTION = [Scope.REQUEST, Scope.TRANSACTION]


class AcmeRequestLedger:
    def __init__(self, session: AcmeBuiltSession) -> None:
        self.session = session


class AcmeSessionAudit:
    def __init__(self, session: AcmeBuiltSession) -> None:
        self.session = session


class AcmeSlowSession:
    """Counts its builds; slow, so threads that start together all miss the cache."""

    built = 0

    def __init__(self) -> None:
        AcmeSlowSession.built += 1
        threading.Event().wait(0.01)


def _bound(scope: Scope, provider: Callable[[], object] | type = AcmeBuiltSession) -> LoomContainer:
    container = LoomContainer()
    container.register(AcmeBuiltSession, provider, scope=scope)
    container.register(AcmeRequestLedger, AcmeRequestLedger, scope=scope)
    container.register(AcmeSessionAudit, AcmeSessionAudit, scope=scope)
    return container


class TestPerExecutionScope:
    @pytest.mark.parametrize("scope", PER_EXECUTION)
    @pytest.mark.parametrize(
        "provider", [AcmeBuiltSession, lambda: AcmeBuiltSession()], ids=["class", "provider"]
    )
    def test_one_instance_per_execution(self, scope: Scope, provider: Callable[[], object]) -> None:
        container = _bound(scope, provider)

        with ExecutionScope(ANONYMOUS):
            first, second = container.resolve(AcmeBuiltSession), container.resolve(AcmeBuiltSession)

        assert first is second

    @pytest.mark.parametrize("scope", PER_EXECUTION)
    def test_the_execution_graph_shares_its_instance(self, scope: Scope) -> None:
        container = _bound(scope)

        with ExecutionScope(ANONYMOUS):
            ledger, audit = (
                container.resolve(AcmeRequestLedger),
                container.resolve(AcmeSessionAudit),
            )
            session = container.resolve(AcmeBuiltSession)

        assert ledger.session is audit.session is session

    @pytest.mark.parametrize("scope", PER_EXECUTION)
    def test_two_executions_never_share_an_instance(self, scope: Scope) -> None:
        container = _bound(scope)

        with ExecutionScope(ANONYMOUS):
            first = container.resolve(AcmeBuiltSession)
        with ExecutionScope(ANONYMOUS):
            second = container.resolve(AcmeBuiltSession)

        assert first is not second

    @pytest.mark.parametrize("scope", PER_EXECUTION)
    def test_outside_an_execution_each_resolve_is_fresh(self, scope: Scope) -> None:
        container = _bound(scope)

        assert container.resolve(AcmeBuiltSession) is not container.resolve(AcmeBuiltSession)
        assert (
            container.resolve(AcmeRequestLedger).session
            is not container.resolve(AcmeRequestLedger).session
        )

    @pytest.mark.parametrize("scope", PER_EXECUTION)
    def test_a_nested_execution_gets_its_own_instance(self, scope: Scope) -> None:
        container = _bound(scope)

        with ExecutionScope(ANONYMOUS):
            outer = container.resolve(AcmeBuiltSession)
            with ExecutionScope(ANONYMOUS):
                inner = container.resolve(AcmeBuiltSession)
            again = container.resolve(AcmeBuiltSession)

        assert inner is not outer
        assert again is outer

    async def test_gathered_children_share_the_execution_instance(self) -> None:
        container = _bound(Scope.REQUEST)

        async def child() -> object:
            await asyncio.sleep(0)
            return container.resolve(AcmeBuiltSession)

        with ExecutionScope(ANONYMOUS):
            first = container.resolve(AcmeBuiltSession)
            gathered = await asyncio.gather(child(), child())

        assert all(session is first for session in gathered)

    def test_a_re_registered_key_is_built_anew_inside_the_same_execution(self) -> None:
        container = _bound(Scope.REQUEST)

        with ExecutionScope(ANONYMOUS):
            first = container.resolve(AcmeBuiltSession)
            container.register(AcmeBuiltSession, AcmeBuiltSession)
            second = container.resolve(AcmeBuiltSession)

        assert first is not second

    def test_a_closed_context_shares_nothing(self) -> None:
        container = _bound(Scope.REQUEST)

        def closed() -> tuple[object, object]:
            token = enter(ANONYMOUS)
            leave(token)
            with pytest.raises(RuntimeError):
                leave(token)
            return container.resolve(AcmeBuiltSession), container.resolve(AcmeBuiltSession)

        first, second = contextvars.copy_context().run(closed)

        assert first is not second

    async def test_threads_of_one_execution_build_one_instance_once(self) -> None:
        threads = 4
        AcmeSlowSession.built = 0
        start = threading.Barrier(threads, timeout=5.0)
        container = LoomContainer()
        container.register(AcmeSlowSession, AcmeSlowSession)

        def resolve() -> object:
            start.wait()
            return container.resolve(AcmeSlowSession)

        with ExecutionScope(ANONYMOUS):
            built = await asyncio.wait_for(
                asyncio.gather(*(asyncio.to_thread(resolve) for _ in range(threads))), timeout=5.0
            )
            kept = container.resolve(AcmeSlowSession)

        assert all(session is kept for session in built)
        assert AcmeSlowSession.built == 1

    async def test_run_in_executor_does_not_share_the_execution(self) -> None:
        container = _bound(Scope.REQUEST)
        loop = asyncio.get_running_loop()

        with ExecutionScope(ANONYMOUS):
            own = container.resolve(AcmeBuiltSession)
            other = await asyncio.wait_for(
                loop.run_in_executor(None, container.resolve, AcmeBuiltSession), timeout=5.0
            )

        assert other is not own

    def test_a_build_waiting_on_a_thread_that_builds_another_key_completes(self) -> None:
        container = LoomContainer()
        container.register(AcmeBuiltSession, AcmeBuiltSession)

        def ledger() -> AcmeRequestLedger:
            context, found = contextvars.copy_context(), list[object]()
            worker = threading.Thread(
                target=lambda: found.append(context.run(container.resolve, AcmeBuiltSession)),
                daemon=True,
            )
            worker.start()
            worker.join(5.0)
            assert found, "the worker did not return: the execution's builds are serialised"
            return AcmeRequestLedger(found[0])  # type: ignore[arg-type]

        container.register(AcmeRequestLedger, ledger)
        results: list[object] = []

        def run() -> None:
            with ExecutionScope(ANONYMOUS):
                built = container.resolve(AcmeRequestLedger)
                results.append(built.session is container.resolve(AcmeBuiltSession))

        runner = threading.Thread(target=run, daemon=True)
        runner.start()
        runner.join(10.0)

        assert results == [True]
