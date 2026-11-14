"""The factory, executor and callback log the entry point tests share."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.use_case.factory import UseCaseFactory

from .doubles import AcmeRouted, CallbackLog, probe_factory


@pytest.fixture(autouse=True)
def no_execution_left_open() -> Iterator[None]:
    """No test leaves an execution open or an ambient identity set."""
    yield
    assert current_execution() is None, "an execution was left open"
    assert current_identity() is ANONYMOUS, "an ambient identity was left set"


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
def executor(compiler: UseCaseCompiler) -> RuntimeExecutor:
    return RuntimeExecutor(compiler)
