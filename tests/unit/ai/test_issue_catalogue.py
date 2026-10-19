"""Contracts of the compile-issue catalogue in ``loom.ai.errors``.

The first three tests are the catalogue contract (see
:mod:`tests.helpers.issue_catalogue`). The rest pin decisions the templates
alone do not show:

* ``*_unreachable`` receives the *registered name* of a server or a remote
  agent, never its URL (FR-030a/FR-038). The parameter name is the only
  instruction the next caller reads, so it is pinned here.
* An unknown provider is not an uninstalled one: there is no extra to install,
  and the message must not tell an operator to install one.
* Both invoker-missing issues read the one shared reason, word for word.
"""

from __future__ import annotations

import pytest

from loom.ai.errors import INVOKER_MISSING_REASON, AgentErrorCode
from tests.helpers.issue_catalogue import (
    INDIRECT_CALLS,
    IssueCall,
    check_renders,
    indirect_calls,
    issue_calls,
    missing_covering_tests,
)


@pytest.mark.parametrize("code", list(AgentErrorCode), ids=str)
def test_every_code_renders_from_exactly_its_declared_params(code: AgentErrorCode) -> None:
    check_renders(code)


@pytest.mark.parametrize("call", issue_calls(AgentErrorCode), ids=lambda call: call.location)
def test_every_call_site_passes_exactly_the_params_of_its_code(call: IssueCall) -> None:
    assert call.keywords == AgentErrorCode[call.member].params


def test_every_indirect_call_site_is_listed_with_tests_that_exist() -> None:
    assert indirect_calls("ai") == {site for site in INDIRECT_CALLS if site.startswith("ai/")}
    assert missing_covering_tests("ai") == []


def test_mcp_server_unreachable_names_its_parameter_server_not_url() -> None:
    """A parameter called ``url`` invites the next caller to pass a URL."""
    assert AgentErrorCode.MCP_SERVER_UNREACHABLE.params == {"server", "reason"}


def test_a2a_agent_unreachable_names_its_parameter_agent_not_url() -> None:
    assert AgentErrorCode.A2A_AGENT_UNREACHABLE.params == {"agent", "reason"}


def test_provider_unknown_lists_the_supported_providers_without_telling_to_install() -> None:
    """``PROVIDER_UNKNOWN`` is not ``PROVIDER_NOT_INSTALLED``: no extra exists."""
    issue = AgentErrorCode.PROVIDER_UNKNOWN.issue(
        component="unheard-of", supported=["anthropic", "openai"]
    )

    assert issue.code is AgentErrorCode.PROVIDER_UNKNOWN
    assert issue.component == "unheard-of"
    assert "anthropic, openai" in issue.message
    assert "extra" not in issue.message
    assert "install" not in issue.message


def test_on_output_invoker_missing_keeps_the_original_text_with_the_shared_reason() -> None:
    """The message is byte-identical to the one shipped before the shared constant."""
    issue = AgentErrorCode.ON_OUTPUT_INVOKER_MISSING.issue(
        agents=["a"], reason=INVOKER_MISSING_REASON
    )

    assert issue.message == "agents declare an output hook but no use-case invoker is configured: a"


def test_conversation_invoker_missing_reads_the_shared_reason() -> None:
    """Both invoker-missing issues share one reason, so operators read one wording."""
    issue = AgentErrorCode.CONVERSATION_INVOKER_MISSING.issue(
        agents=["a"], reason=INVOKER_MISSING_REASON
    )

    assert (
        issue.message
        == "agents declare a conversation loader but no use-case invoker is configured: a"
    )
