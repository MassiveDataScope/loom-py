"""The ``Caller()`` marker: a declared identity parameter, bound fail-closed.

The identity is neither a constructor argument (it would not survive a broker
hop) nor an ambient read inside ``execute`` (hidden state).  It is declared in
the signature like every other Loom binding, and the transport hands it to the
executor per execution.
"""

from __future__ import annotations

import asyncio

import pytest

from loom.core.engine.compiler import CompilationError, UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.errors import Unauthenticated
from loom.core.identity import ANONYMOUS, Identity
from loom.core.use_case.markers import Caller
from loom.core.use_case.use_case import UseCase

_ALICE = Identity(subject="alice", roles=("role_reader",), mechanism="test")
_BOB = Identity(subject="bob", mechanism="test")


class WhoAmIUseCase(UseCase[object, str]):
    """Returns the subject of whoever is running it."""

    async def execute(self, caller: Identity = Caller()) -> str:
        return caller.subject


class GreetUseCase(UseCase[object, str]):
    """Mixes a primitive parameter with the declared caller."""

    async def execute(self, greeting: str, caller: Identity = Caller()) -> str:
        return f"{greeting} {caller.subject}"


class NoCallerUseCase(UseCase[object, str]):
    """Declares no identity at all."""

    async def execute(self) -> str:
        return "anyone"


def _executor() -> UseCaseExecutor:
    return UseCaseExecutor(UseCaseCompiler())


async def _run(use_case_type: type[UseCase[object, str]], **kwargs: object) -> str:
    compiler = UseCaseCompiler()
    compiler.compile(use_case_type)
    executor = UseCaseExecutor(compiler)
    result: str = await executor.execute(use_case_type(), **kwargs)  # type: ignore[arg-type]
    return result


# ---------------------------------------------------------------------------
# Compilation
# ---------------------------------------------------------------------------


class TestCallerBinding:
    def test_the_marker_compiles_into_a_caller_binding(self) -> None:
        """``Caller()`` is recognised at startup, exactly like the other markers."""
        plan = UseCaseCompiler().compile(WhoAmIUseCase)
        assert plan.caller_binding is not None
        assert plan.caller_binding.name == "caller"

    def test_the_caller_parameter_is_not_a_primitive_parameter(self) -> None:
        """It must not fall through to ``ParamBinding``, or callers would supply it."""
        plan = UseCaseCompiler().compile(WhoAmIUseCase)
        assert [pb.name for pb in plan.param_bindings] == []

    def test_primitive_parameters_still_compile_alongside_the_caller(self) -> None:
        """Declaring an identity does not disturb the rest of the signature."""
        plan = UseCaseCompiler().compile(GreetUseCase)
        assert [pb.name for pb in plan.param_bindings] == ["greeting"]

    def test_a_use_case_without_the_marker_has_no_caller_binding(self) -> None:
        """The binding is opt-in: nothing changes for use cases that ignore it."""
        plan = UseCaseCompiler().compile(NoCallerUseCase)
        assert plan.caller_binding is None

    def test_two_caller_parameters_fail_at_startup(self) -> None:
        """A second ``Caller()`` would be silently dropped: refuse it at compile time."""

        class TwoCallersUseCase(UseCase[object, str]):
            async def execute(self, a: Identity = Caller(), b: Identity = Caller()) -> str:
                return f"{a.subject}{b.subject}"

        compiler = UseCaseCompiler()
        with pytest.raises(CompilationError, match="Caller"):
            compiler.compile(TwoCallersUseCase)

    # ---------------------------------------------------------------------------
    # Execution
    # ---------------------------------------------------------------------------

    async def test_the_declared_identity_reaches_execute(self) -> None:
        """The executor injects exactly the identity the transport handed it."""
        assert await _run(WhoAmIUseCase, identity=_ALICE) == "alice"

    async def test_the_identity_coexists_with_primitive_parameters(self) -> None:
        """Params and identity are bound independently."""
        result = await _run(GreetUseCase, params={"greeting": "hi"}, identity=_ALICE)
        assert result == "hi alice"

    async def test_an_explicit_anonymous_identity_is_delivered_as_such(self) -> None:
        """An anonymous caller is a legitimate answer; the use case decides what to do."""
        assert await _run(WhoAmIUseCase, identity=ANONYMOUS) == ""

    async def test_a_missing_identity_is_refused_instead_of_defaulted(self) -> None:
        """Fail-closed: a transport that delivers no identity never yields ANONYMOUS."""
        compiler = UseCaseCompiler()
        compiler.compile(WhoAmIUseCase)
        executor = UseCaseExecutor(compiler)
        use_case = WhoAmIUseCase()

        with pytest.raises(Unauthenticated):
            await executor.execute(use_case)

    async def test_the_refusal_names_the_use_case_and_the_parameter(self) -> None:
        """The message must tell a developer which wiring is missing."""
        compiler = UseCaseCompiler()
        compiler.compile(WhoAmIUseCase)
        executor = UseCaseExecutor(compiler)
        use_case = WhoAmIUseCase()

        with pytest.raises(Unauthenticated, match="WhoAmIUseCase"):
            await executor.execute(use_case)

    async def test_an_identity_passed_to_a_use_case_that_declares_none_is_ignored(self) -> None:
        """Transports always pass the identity; only declared parameters receive it."""
        assert await _run(NoCallerUseCase, identity=_ALICE) == "anyone"

    async def test_concurrent_executions_never_cross_identities(self) -> None:
        """Two callers running at once must each see their own identity."""
        compiler = UseCaseCompiler()
        compiler.compile(WhoAmIUseCase)
        executor = UseCaseExecutor(compiler)

        results = await asyncio.gather(
            executor.execute(WhoAmIUseCase(), identity=_ALICE),
            executor.execute(WhoAmIUseCase(), identity=_BOB),
        )

        assert results == ["alice", "bob"]

    def test_the_executor_is_constructible_without_identity_support(self) -> None:
        """The parameter is additive: existing composition roots keep working."""
        assert isinstance(_executor(), UseCaseExecutor)
