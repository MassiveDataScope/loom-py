"""Contracts of the compile-issue catalogue in ``loom.ai.errors``.

The catalogue contract every pillar keeps is checked in
``tests/unit/core/errors/test_issue_catalogues.py``. These tests pin decisions
the templates alone do not show:

* ``*_unreachable`` receives the *registered name* of a server or a remote
  agent, never its URL (FR-030a/FR-038). The parameter name is the only
  instruction the next caller reads, so it is pinned here.
* An unknown provider is not an uninstalled one: there is no extra to install,
  and the message must not tell an operator to install one.
* Both invoker-missing issues read the one shared reason, word for word.
"""

from __future__ import annotations

from loom.ai.errors import INVOKER_MISSING_REASON, AgentErrorCode


class TestIssueCatalogueMessages:
    def test_mcp_server_unreachable_names_its_parameter_server_not_url(self) -> None:
        """A parameter called ``url`` invites the next caller to pass a URL."""
        assert AgentErrorCode.MCP_SERVER_UNREACHABLE.params == {"server", "reason"}

    def test_a2a_agent_unreachable_names_its_parameter_agent_not_url(self) -> None:
        assert AgentErrorCode.A2A_AGENT_UNREACHABLE.params == {"agent", "reason"}

    def test_provider_unknown_lists_the_supported_providers_without_telling_to_install(
        self,
    ) -> None:
        """``PROVIDER_UNKNOWN`` is not ``PROVIDER_NOT_INSTALLED``: no extra exists."""
        issue = AgentErrorCode.PROVIDER_UNKNOWN.issue(
            component="unheard-of", supported=["anthropic", "openai"]
        )

        assert issue.code is AgentErrorCode.PROVIDER_UNKNOWN
        assert issue.component == "unheard-of"
        assert "anthropic, openai" in issue.message
        assert "extra" not in issue.message
        assert "install" not in issue.message

    def test_on_output_invoker_missing_keeps_the_original_text_with_the_shared_reason(self) -> None:
        """The message is byte-identical to the one shipped before the shared constant."""
        issue = AgentErrorCode.ON_OUTPUT_INVOKER_MISSING.issue(
            agents=["a"], reason=INVOKER_MISSING_REASON
        )

        assert (
            issue.message
            == "agents declare an output hook but no use-case invoker is configured: a"
        )

    def test_conversation_invoker_missing_reads_the_shared_reason(self) -> None:
        """Both invoker-missing issues share one reason, so operators read one wording."""
        issue = AgentErrorCode.CONVERSATION_INVOKER_MISSING.issue(
            agents=["a"], reason=INVOKER_MISSING_REASON
        )

        assert (
            issue.message
            == "agents declare a conversation loader but no use-case invoker is configured: a"
        )
