from __future__ import annotations

import pytest

from loom.core.identity import Identity
from loom.testing.golden import GoldenHarness
from loom.testing.runner import AgentHandleDouble, McpHandleDouble
from tests.unit.testing.doubles import AgentTriage


@pytest.fixture(autouse=True)
def _no_adapter_built(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(AgentTriage, "built", 0)


@pytest.fixture
def caller() -> Identity:
    return Identity(subject="ada")


@pytest.fixture
def harness() -> GoldenHarness:
    return GoldenHarness()


@pytest.fixture
def triage_agent() -> AgentHandleDouble:
    return AgentHandleDouble("triage").on_run("sev-2")


@pytest.fixture
def runbooks() -> McpHandleDouble:
    return (
        McpHandleDouble("runbooks")
        .with_tools("search")
        .on_call_untyped("search", {"title": "restart the broker"})
    )
