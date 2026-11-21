"""Executors, stores and recorders the executor tests share; no execution outlives a test."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from loom.core.engine import observation
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.uow.coordinator import OrderedCommits
from loom.core.use_case.factory import UseCaseFactory
from tests.helpers.loggers import CapturedLog

from ...uow._coordinator_fakes import Ledger
from .._lifecycle_doubles import RecordingExecutionObserver
from .doubles import (
    JOURNAL,
    AcmeStores,
    McpCalls,
    mcp_resolver,
    session_factory,
)

MakeExecutor = Callable[..., UseCaseExecutor]


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

    def make(**options: Any) -> UseCaseExecutor:
        defaults: dict[str, Any] = {"execution_observer": JOURNAL}
        return UseCaseExecutor(UseCaseCompiler(), **{**defaults, **options})

    return make


@pytest.fixture
def executor(make_executor: MakeExecutor) -> UseCaseExecutor:
    return make_executor()


@pytest.fixture
def logger(caplog: pytest.LogCaptureFixture) -> CapturedLog:
    """What the executor reports through its logger during the test."""
    caplog.set_level(logging.DEBUG, logger=observation.__name__)
    return CapturedLog(caplog, observation.__name__)


@pytest.fixture
def observer() -> RecordingExecutionObserver:
    return RecordingExecutionObserver()


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
def transactional_executor(
    stores: AcmeStores, observer: RecordingExecutionObserver
) -> UseCaseExecutor:
    """An executor whose top-level writers open an invocation scope over ``stores``."""
    return UseCaseExecutor(
        UseCaseCompiler(),
        coordinator=OrderedCommits({DEFAULT_STORE: stores}),
        execution_observer=observer,
    )


@pytest.fixture
def mcp_calls() -> McpCalls:
    return []


@pytest.fixture
def mcp_executor(mcp_calls: McpCalls) -> UseCaseExecutor:
    """An executor whose ``Mcp()`` resolver records each call in ``mcp_calls``."""
    executor = UseCaseExecutor(UseCaseCompiler())
    executor.bind_mcp_resolver(mcp_resolver(mcp_calls))
    return executor
