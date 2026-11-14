"""Executors, stores and recorders the executor tests share; no execution outlives a test."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.uow.coordinator import OrderedCommits
from loom.core.use_case.factory import UseCaseFactory

from ...uow._coordinator_fakes import Ledger
from .._lifecycle_doubles import Metrics
from .doubles import (
    JOURNAL,
    AcmeRecordingLogger,
    AcmeStores,
    McpCalls,
    mcp_resolver,
    session_factory,
)

MakeExecutor = Callable[..., RuntimeExecutor]


@pytest.fixture(autouse=True)
def no_execution_left_open() -> Iterator[None]:
    """Each test starts with an empty journal; none leaves an execution or an identity set."""
    JOURNAL.entries.clear()
    yield
    assert current_execution() is None, "an execution was left open"
    assert current_identity() is ANONYMOUS, "an ambient identity was left set"


@pytest.fixture
def make_executor() -> MakeExecutor:
    """An executor reporting its events to ``JOURNAL`` and logging to a recorder, by default."""

    def make(**options: Any) -> RuntimeExecutor:
        defaults: dict[str, Any] = {"logger": AcmeRecordingLogger(), "metrics": JOURNAL}
        return RuntimeExecutor(UseCaseCompiler(), **{**defaults, **options})

    return make


@pytest.fixture
def executor(make_executor: MakeExecutor) -> RuntimeExecutor:
    return make_executor()


@pytest.fixture
def logger() -> AcmeRecordingLogger:
    return AcmeRecordingLogger()


@pytest.fixture
def metrics() -> Metrics:
    return Metrics()


@pytest.fixture
def factory() -> UseCaseFactory:
    """A factory whose container serves ``AcmeSession`` per execution."""
    return session_factory()


@pytest.fixture
def ledger() -> Ledger:
    return Ledger()


@pytest.fixture
def stores(ledger: Ledger) -> AcmeStores:
    return AcmeStores(ledger)


@pytest.fixture
def transactional_executor(stores: AcmeStores, metrics: Metrics) -> RuntimeExecutor:
    """An executor whose top-level writers open an invocation scope over ``stores``."""
    return RuntimeExecutor(
        UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: stores}), metrics=metrics
    )


@pytest.fixture
def mcp_calls() -> McpCalls:
    return []


@pytest.fixture
def mcp_executor(mcp_calls: McpCalls) -> RuntimeExecutor:
    """An executor whose ``Mcp()`` resolver records each call in ``mcp_calls``."""
    executor = RuntimeExecutor(UseCaseCompiler())
    executor.bind_mcp_resolver(mcp_resolver(mcp_calls))
    return executor
