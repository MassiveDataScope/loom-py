"""The factory, executor and callback log the entry point tests share."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS, Identity, current_identity, reset_identity, set_identity
from loom.core.use_case.factory import UseCaseFactory

from .doubles import ALICE, AcmeRouted, CallbackLog, probe_factory


@pytest.fixture(autouse=True)
def no_execution_left_open() -> Iterator[None]:
    """No test leaves an execution open or an ambient identity set."""
    yield
    assert current_execution() is None, "an execution was left open"
    assert current_identity() is ANONYMOUS, "an ambient identity was left set"


@pytest.fixture
def as_alice() -> Iterator[None]:
    """The ambient identity is alice, as a transport that authenticated her sets it."""
    token = set_identity(ALICE)
    try:
        yield
    finally:
        reset_identity(token)


@pytest.fixture
def authenticated(caller: Identity | None) -> Iterator[None]:
    """The transport authenticated the request as ``caller`` (from ``CALLERS``), if any."""
    token = None if caller is None else set_identity(caller)
    try:
        yield
    finally:
        if token is not None:
            reset_identity(token)


@pytest.fixture
def callback_log() -> CallbackLog:
    return CallbackLog()


@pytest.fixture
def factory(callback_log: CallbackLog) -> UseCaseFactory:
    return probe_factory(callback_log)


@pytest.fixture
def compiler() -> UseCaseCompiler:
    """A compiler that has compiled the routed use case, as a kernel would."""
    compiler = UseCaseCompiler()
    compiler.compile(AcmeRouted)
    return compiler


@pytest.fixture
def executor(compiler: UseCaseCompiler) -> UseCaseExecutor:
    return UseCaseExecutor(compiler)
