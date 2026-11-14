"""H5: what an adapter declares and cannot be satisfied fails at startup, naming its path.

The server and agent names are checked when the AI phase composes, before
seal; the tool filter when the runtime lists the server at startup. A host
without agents refuses an adapter's declaration as it refuses one in
``execute()``.
"""

from __future__ import annotations

from typing import Any

import pytest

import loom.composition.pipeline as pipeline
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from tests.integration.ai.adapters.conftest import AgentlessHost, Compose
from tests.integration.ai.adapters.doubles import (
    PATH_FROM_CALLBACK,
    PATH_TO_ANALYST,
    PATH_TO_GATEWAY,
    AcmeAuditCallback,
    AcmeFakeAdvice,
    AcmeFakeQuotes,
    AcmeMarket,
    AcmeNarrowQuotes,
    AcmeTypoAdvice,
    AcmeTypoQuotes,
)


@pytest.fixture
def seals(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Every application sealed while the test runs."""
    sealed: list[object] = []
    seal = pipeline.seal

    def recorded(registered: Any) -> None:
        sealed.append(registered)
        seal(registered)

    monkeypatch.setattr(pipeline, "seal", recorded)
    return sealed


class TestUnknownNameThroughAnAdapter:
    @pytest.mark.parametrize(
        ("bindings", "code", "path"),
        [
            (
                {"quotes": AcmeTypoQuotes, "advice": AcmeFakeAdvice()},
                AgentErrorCode.MCP_MARKER_UNKNOWN,
                PATH_TO_GATEWAY,
            ),
            (
                {"quotes": AcmeFakeQuotes(), "advice": AcmeTypoAdvice},
                AgentErrorCode.AGENT_MARKER_UNKNOWN,
                PATH_TO_ANALYST,
            ),
        ],
        ids=["server", "agent"],
    )
    def test_fails_in_the_ai_phase_naming_the_path(
        self,
        compose: Compose,
        seals: list[object],
        bindings: dict[str, object],
        code: AgentErrorCode,
        path: str,
    ) -> None:
        with pytest.raises(AgentCompilationError) as raised:
            compose(**bindings)

        assert {issue.code for issue in raised.value.issues} == {code}
        assert path in str(raised.value)
        assert seals == []


class TestUnknownToolThroughAnAdapter:
    async def test_fails_when_the_runtime_starts_naming_the_path(self, compose: Compose) -> None:
        runtime = compose(quotes=AcmeNarrowQuotes, advice=AcmeFakeAdvice()).ai.runtime
        assert runtime is not None

        with pytest.raises(AgentCompilationError) as raised:
            async with runtime:
                pass

        codes = {issue.code for issue in raised.value.issues}
        assert codes == {AgentErrorCode.USECASE_TOOL_FILTER_MATCHES_NOTHING}
        assert PATH_TO_GATEWAY in str(raised.value)


class TestHostWithoutAgents:
    @pytest.mark.parametrize(
        ("bindings", "code", "path"),
        [
            (
                {"advice": AcmeFakeAdvice()},
                AgentErrorCode.MCP_MARKER_UNKNOWN,
                PATH_TO_GATEWAY,
            ),
            (
                {"quotes": AcmeFakeQuotes()},
                AgentErrorCode.AGENT_MARKER_UNKNOWN,
                PATH_TO_ANALYST,
            ),
        ],
        ids=["server", "agent"],
    )
    def test_refuses_an_adapter_declaration_naming_the_path(
        self,
        compose: Compose,
        seals: list[object],
        bindings: dict[str, object],
        code: AgentErrorCode,
        path: str,
    ) -> None:
        with pytest.raises(AgentCompilationError) as raised:
            compose(host=AgentlessHost((AcmeMarket,)), **bindings)

        assert {issue.code for issue in raised.value.issues} == {code}
        assert path in str(raised.value)
        assert seals == []

    def test_refuses_a_callback_reaching_an_adapter_declaration(
        self, compose: Compose, seals: list[object]
    ) -> None:
        with pytest.raises(AgentCompilationError) as raised:
            compose(host=AgentlessHost((), callbacks=(AcmeAuditCallback,)), advice=AcmeFakeAdvice())

        codes = {issue.code for issue in raised.value.issues}
        assert codes == {AgentErrorCode.MCP_MARKER_UNKNOWN}
        assert PATH_FROM_CALLBACK in str(raised.value)
        assert seals == []
