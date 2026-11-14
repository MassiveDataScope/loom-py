"""Pinning authorization from the test harnesses.

A use case that reads its caller must be testable without an HTTP stack, and
the harnesses must not paper over the fail-closed rule: forgetting to state the
caller has to fail, or every authorization test would be vacuous.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.errors import Unauthenticated
from loom.core.identity import ANONYMOUS, Identity
from loom.core.use_case.markers import Caller
from loom.core.use_case.use_case import UseCase
from loom.testing.golden import GoldenHarness, serialize_plan
from loom.testing.runner import UseCaseTest

_SUBJECT = "user-1"


class WhoAmIUseCase(UseCase[Any, str]):
    """Returns the subject of whoever runs it."""

    async def execute(self, caller: Identity = Caller()) -> str:
        return caller.subject


def _caller() -> Identity:
    return Identity(subject=_SUBJECT, roles=("reader",), mechanism="test")


# ---------------------------------------------------------------------------
# UseCaseTest
# ---------------------------------------------------------------------------


class TestIdentityHarness:
    async def test_the_runner_executes_as_the_declared_caller(self) -> None:
        """``with_caller`` fills the declared identity parameter."""
        result = await UseCaseTest(WhoAmIUseCase()).with_caller(_caller()).run()
        assert result == _SUBJECT

    async def test_the_runner_can_pin_the_anonymous_path(self) -> None:
        """Stating ANONYMOUS explicitly is how an unauthenticated test is written."""
        result = await UseCaseTest(WhoAmIUseCase()).with_caller(ANONYMOUS).run()
        assert result == ""

    async def test_the_runner_fails_closed_without_a_caller(self) -> None:
        """Forgetting the caller must fail, not silently authorize."""
        harness = UseCaseTest(WhoAmIUseCase())
        with pytest.raises(Unauthenticated):
            await harness.run()

    # ---------------------------------------------------------------------------
    # GoldenHarness
    # ---------------------------------------------------------------------------

    async def test_the_golden_harness_executes_as_the_declared_caller(self) -> None:
        """Golden runs pin behaviour per caller, not per anonymous request."""
        result = await GoldenHarness().run(WhoAmIUseCase, identity=_caller())
        assert result == _SUBJECT

    async def test_the_golden_harness_fails_closed_without_a_caller(self) -> None:
        """The harness inherits the executor rule instead of softening it."""
        harness = GoldenHarness()
        with pytest.raises(Unauthenticated):
            await harness.run(WhoAmIUseCase)

    def test_the_plan_snapshot_records_the_caller_binding(self) -> None:
        """A golden plan must show that a use case reads its caller."""
        plan = UseCaseCompiler().compile(WhoAmIUseCase)
        assert serialize_plan(plan)["caller_binding"] == {"name": "caller"}
