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

PER_EXECUTION = [Scope.REQUEST, Scope.TRANSACTION]


class AcmeSession:
    """Built per execution."""


class AcmeLedger:
    def __init__(self, session: AcmeSession) -> None:
        self.session = session


class AcmeAudit:
    def __init__(self, session: AcmeSession) -> None:
        self.session = session


def _bound(scope: Scope, provider: Callable[[], object] | type = AcmeSession) -> LoomContainer:
    container = LoomContainer()
    container.register(AcmeSession, provider, scope=scope)
    container.register(AcmeLedger, AcmeLedger, scope=scope)
    container.register(AcmeAudit, AcmeAudit, scope=scope)
    return container


@pytest.mark.parametrize("scope", PER_EXECUTION)
@pytest.mark.parametrize(
    "provider", [AcmeSession, lambda: AcmeSession()], ids=["class", "provider"]
)
def test_one_instance_per_execution(scope: Scope, provider: Callable[[], object]) -> None:
    container = _bound(scope, provider)

    with ExecutionScope(ANONYMOUS):
        first, second = container.resolve(AcmeSession), container.resolve(AcmeSession)

    assert first is second


@pytest.mark.parametrize("scope", PER_EXECUTION)
def test_the_execution_graph_shares_its_instance(scope: Scope) -> None:
    container = _bound(scope)

    with ExecutionScope(ANONYMOUS):
        ledger, audit = container.resolve(AcmeLedger), container.resolve(AcmeAudit)
        session = container.resolve(AcmeSession)

    assert ledger.session is audit.session is session


@pytest.mark.parametrize("scope", PER_EXECUTION)
def test_two_executions_never_share_an_instance(scope: Scope) -> None:
    container = _bound(scope)

    with ExecutionScope(ANONYMOUS):
        first = container.resolve(AcmeSession)
    with ExecutionScope(ANONYMOUS):
        second = container.resolve(AcmeSession)

    assert first is not second


@pytest.mark.parametrize("scope", PER_EXECUTION)
def test_outside_an_execution_each_resolve_is_fresh(scope: Scope) -> None:
    container = _bound(scope)

    assert container.resolve(AcmeSession) is not container.resolve(AcmeSession)
    assert container.resolve(AcmeLedger).session is not container.resolve(AcmeLedger).session


@pytest.mark.parametrize("scope", PER_EXECUTION)
def test_a_nested_execution_gets_its_own_instance(scope: Scope) -> None:
    container = _bound(scope)

    with ExecutionScope(ANONYMOUS):
        outer = container.resolve(AcmeSession)
        with ExecutionScope(ANONYMOUS):
            inner = container.resolve(AcmeSession)
        again = container.resolve(AcmeSession)

    assert inner is not outer
    assert again is outer


async def test_gathered_children_share_the_execution_instance() -> None:
    container = _bound(Scope.REQUEST)

    async def child() -> object:
        await asyncio.sleep(0)
        return container.resolve(AcmeSession)

    with ExecutionScope(ANONYMOUS):
        first = container.resolve(AcmeSession)
        gathered = await asyncio.gather(child(), child())

    assert all(session is first for session in gathered)


def test_a_re_registered_key_is_built_anew_inside_the_same_execution() -> None:
    container = _bound(Scope.REQUEST)

    with ExecutionScope(ANONYMOUS):
        first = container.resolve(AcmeSession)
        container.register(AcmeSession, AcmeSession)
        second = container.resolve(AcmeSession)

    assert first is not second


def test_a_closed_context_shares_nothing() -> None:
    container = _bound(Scope.REQUEST)

    def closed() -> tuple[object, object]:
        token = enter(ANONYMOUS)
        leave(token)
        with pytest.raises(RuntimeError):
            leave(token)
        return container.resolve(AcmeSession), container.resolve(AcmeSession)

    first, second = contextvars.copy_context().run(closed)

    assert first is not second


class AcmeSlowSession:
    """Counts its builds; slow, so threads that start together all miss the cache."""

    built = 0

    def __init__(self) -> None:
        AcmeSlowSession.built += 1
        threading.Event().wait(0.01)


async def test_threads_of_one_execution_build_one_instance_once() -> None:
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


async def test_run_in_executor_does_not_share_the_execution() -> None:
    container = _bound(Scope.REQUEST)
    loop = asyncio.get_running_loop()

    with ExecutionScope(ANONYMOUS):
        own = container.resolve(AcmeSession)
        other = await asyncio.wait_for(
            loop.run_in_executor(None, container.resolve, AcmeSession), timeout=5.0
        )

    assert other is not own


def test_a_build_waiting_on_a_thread_that_builds_another_key_completes() -> None:
    container = LoomContainer()
    container.register(AcmeSession, AcmeSession)

    def ledger() -> AcmeLedger:
        context, found = contextvars.copy_context(), list[object]()
        worker = threading.Thread(
            target=lambda: found.append(context.run(container.resolve, AcmeSession)), daemon=True
        )
        worker.start()
        worker.join(5.0)
        assert found, "the worker did not return: the execution's builds are serialised"
        return AcmeLedger(found[0])  # type: ignore[arg-type]

    container.register(AcmeLedger, ledger)
    results: list[object] = []

    def run() -> None:
        with ExecutionScope(ANONYMOUS):
            built = container.resolve(AcmeLedger)
            results.append(built.session is container.resolve(AcmeSession))

    runner = threading.Thread(target=run, daemon=True)
    runner.start()
    runner.join(10.0)

    assert results == [True]
