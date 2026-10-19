"""Contracts of the compile-issue catalogue in ``loom.ai.errors``.

Every code renders from exactly its declared params, and every call site in
``src/loom`` passes exactly those params: the check the type checker made on
the old factory signatures.

Two facts the catalogue must keep, because both are load-bearing for the
redaction guarantee (FR-030a/FR-038) and for the "every problem names the
offending field" principle:

* ``*_unreachable`` receives the *registered name* of a server or a remote
  agent, never its URL. The parameter name is the only instruction the next
  caller reads, so it is pinned here.
* An unknown provider is not an uninstalled one: there is no extra to install,
  and the message must not tell an operator to install one.
"""

from __future__ import annotations

import pytest

from loom.ai.errors import INVOKER_MISSING_REASON, AgentErrorCode
from tests.helpers.issue_catalogue import IssueCall, check_renders, issue_calls


@pytest.mark.parametrize("code", list(AgentErrorCode), ids=str)
def test_every_code_renders_from_exactly_its_declared_params(code: AgentErrorCode) -> None:
    check_renders(code)


@pytest.mark.parametrize("call", issue_calls(AgentErrorCode), ids=lambda call: call.location)
def test_every_call_site_passes_exactly_the_params_of_its_code(call: IssueCall) -> None:
    assert call.keywords == AgentErrorCode[call.member].params


def test_mcp_server_unreachable_names_its_parameter_server_not_url() -> None:
    """A parameter called ``url`` invites the next caller to pass a URL."""
    assert AgentErrorCode.MCP_SERVER_UNREACHABLE.params == {"server", "reason"}


def test_a2a_agent_unreachable_names_its_parameter_agent_not_url() -> None:
    assert AgentErrorCode.A2A_AGENT_UNREACHABLE.params == {"agent", "reason"}


def test_mcp_server_unreachable_carries_the_registered_name_to_the_component() -> None:
    issue = AgentErrorCode.MCP_SERVER_UNREACHABLE.issue(
        server="reporting-mcp", reason="connection refused"
    )

    assert issue.code is AgentErrorCode.MCP_SERVER_UNREACHABLE
    assert issue.component == "reporting-mcp"
    assert "reporting-mcp" in issue.message


def test_a2a_agent_unreachable_carries_the_registered_name_to_the_component() -> None:
    issue = AgentErrorCode.A2A_AGENT_UNREACHABLE.issue(
        agent="pricing-desk", reason="card not retrievable"
    )

    assert issue.code is AgentErrorCode.A2A_AGENT_UNREACHABLE
    assert issue.component == "pricing-desk"
    assert "pricing-desk" in issue.message


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


def test_on_output_usecase_unknown_points_at_the_on_output_usecase_field() -> None:
    """The unknown key is attributed to the hook field, not to the capabilities."""
    issue = AgentErrorCode.ON_OUTPUT_USECASE_UNKNOWN.issue(
        component="triage-bot", key="incidents.record_triage"
    )

    assert issue.code is AgentErrorCode.ON_OUTPUT_USECASE_UNKNOWN
    assert issue.component == "triage-bot"
    assert issue.field == "on_output.usecase"
    assert "incidents.record_triage" in issue.message


def test_on_output_input_unsatisfied_carries_the_reason_in_the_message() -> None:
    """The reason is the only clue the author gets about which Input field fails."""
    issue = AgentErrorCode.ON_OUTPUT_INPUT_UNSATISFIED.issue(
        component="triage-bot",
        key="incidents.record_triage",
        reason="field 'reviewer_email' has no default",
    )

    assert issue.code is AgentErrorCode.ON_OUTPUT_INPUT_UNSATISFIED
    assert issue.component == "triage-bot"
    assert issue.field == "on_output.usecase"
    assert "incidents.record_triage" in issue.message
    assert "field 'reviewer_email' has no default" in issue.message


def test_on_output_usecase_also_granted_points_at_the_on_output_usecase_field() -> None:
    """A key that is both hook and capability is reported once, on the hook field."""
    issue = AgentErrorCode.ON_OUTPUT_USECASE_ALSO_GRANTED.issue(
        component="triage-bot", key="incidents.record_triage"
    )

    assert issue.code is AgentErrorCode.ON_OUTPUT_USECASE_ALSO_GRANTED
    assert issue.component == "triage-bot"
    assert issue.field == "on_output.usecase"
    assert "incidents.record_triage" in issue.message


def test_on_output_invoker_missing_is_a_deployment_issue_naming_the_agents() -> None:
    """No single agent owns the missing invoker, so the issue belongs to ``ai``."""
    issue = AgentErrorCode.ON_OUTPUT_INVOKER_MISSING.issue(
        agents=["triage-bot", "escalation-bot"], reason=INVOKER_MISSING_REASON
    )

    assert issue.code is AgentErrorCode.ON_OUTPUT_INVOKER_MISSING
    assert issue.component == "ai"
    assert issue.field == "on_output"
    assert "triage-bot, escalation-bot" in issue.message


def test_on_output_invoker_missing_keeps_the_original_text_with_the_shared_reason() -> None:
    """The message is byte-identical to the one shipped before the shared constant."""
    issue = AgentErrorCode.ON_OUTPUT_INVOKER_MISSING.issue(
        agents=["a"], reason=INVOKER_MISSING_REASON
    )

    assert INVOKER_MISSING_REASON in issue.message
    assert issue.message == "agents declare an output hook but no use-case invoker is configured: a"


def test_conversation_usecase_unknown_points_at_the_conversation_usecase_field() -> None:
    """The unknown key is attributed to the loader field, not to the capabilities."""
    issue = AgentErrorCode.CONVERSATION_USECASE_UNKNOWN.issue(
        component="triage-bot", key="conversations.load"
    )

    assert issue.code is AgentErrorCode.CONVERSATION_USECASE_UNKNOWN
    assert issue.component == "triage-bot"
    assert issue.field == "conversation.usecase"
    assert "conversations.load" in issue.message


def test_conversation_input_unsatisfied_carries_the_reason_in_the_message() -> None:
    """The reason is the only clue the author gets about which Input field fails."""
    issue = AgentErrorCode.CONVERSATION_INPUT_UNSATISFIED.issue(
        component="triage-bot", key="conversations.load", reason="field 'tenant' has no default"
    )

    assert issue.code is AgentErrorCode.CONVERSATION_INPUT_UNSATISFIED
    assert issue.component == "triage-bot"
    assert issue.field == "conversation.usecase"
    assert "conversations.load" in issue.message
    assert "field 'tenant' has no default" in issue.message


def test_conversation_usecase_also_granted_points_at_the_conversation_usecase_field() -> None:
    """A key that is both loader and capability is reported once, on the loader field."""
    issue = AgentErrorCode.CONVERSATION_USECASE_ALSO_GRANTED.issue(
        component="triage-bot", key="conversations.load"
    )

    assert issue.code is AgentErrorCode.CONVERSATION_USECASE_ALSO_GRANTED
    assert issue.component == "triage-bot"
    assert issue.field == "conversation.usecase"
    assert "conversations.load" in issue.message


def test_conversation_invoker_missing_is_a_deployment_issue_naming_the_agents() -> None:
    """No single agent owns the missing invoker, so the issue belongs to ``ai``."""
    issue = AgentErrorCode.CONVERSATION_INVOKER_MISSING.issue(
        agents=["triage-bot", "escalation-bot"], reason=INVOKER_MISSING_REASON
    )

    assert issue.code is AgentErrorCode.CONVERSATION_INVOKER_MISSING
    assert issue.component == "ai"
    assert issue.field == "conversation"
    assert "triage-bot, escalation-bot" in issue.message


def test_conversation_invoker_missing_reads_the_shared_reason() -> None:
    """Both invoker-missing issues share one reason, so operators read one wording."""
    issue = AgentErrorCode.CONVERSATION_INVOKER_MISSING.issue(
        agents=["a"], reason=INVOKER_MISSING_REASON
    )

    assert INVOKER_MISSING_REASON in issue.message
    assert (
        issue.message
        == "agents declare a conversation loader but no use-case invoker is configured: a"
    )


def test_conversation_invoker_missing_carries_the_reason_when_given() -> None:
    """An invoker that exists but is unusable is reported with its own reason."""
    issue = AgentErrorCode.CONVERSATION_INVOKER_MISSING.issue(
        agents=["triage-bot"], reason="the use-case invoker is unbound"
    )

    assert issue.code is AgentErrorCode.CONVERSATION_INVOKER_MISSING
    assert "the use-case invoker is unbound" in issue.message
    assert "triage-bot" in issue.message


def test_mcp_transport_invalid_names_the_component_and_carries_the_reason() -> None:
    """The reason is the only clue the operator gets about which transport rule fails."""
    issue = AgentErrorCode.MCP_TRANSPORT_INVALID.issue(
        component="ai.mcp_servers.search", reason="transport 'ws' is not supported"
    )

    assert issue.code is AgentErrorCode.MCP_TRANSPORT_INVALID
    assert issue.component == "ai.mcp_servers.search"
    assert issue.field == "transport"
    assert "ai.mcp_servers.search" in issue.message
    assert "transport 'ws' is not supported" in issue.message


def test_agent_marker_unknown_points_at_the_use_cases_parameter() -> None:
    """The offending field is the parameter, not the use case or the agent."""
    issue = AgentErrorCode.AGENT_MARKER_UNKNOWN.issue(
        component="incidents.report",
        parameter="triage",
        agent="incident-triage",
        available=["escalation-bot"],
    )

    assert issue.code is AgentErrorCode.AGENT_MARKER_UNKNOWN
    assert issue.component == "incidents.report"
    assert issue.field == "parameters.triage"
    assert "incident-triage" in issue.message
    assert "escalation-bot" in issue.message


def test_agent_marker_unknown_declares_none_when_no_agents_are_compiled() -> None:
    """An empty deployment still produces a readable message."""
    issue = AgentErrorCode.AGENT_MARKER_UNKNOWN.issue(
        component="incidents.report", parameter="triage", agent="incident-triage", available=[]
    )

    assert "none" in issue.message


def test_mcp_marker_unknown_points_at_the_use_cases_parameter() -> None:
    """The offending field is the parameter, not the use case or the server.

    ``parameter`` and ``server`` are deliberately disjoint strings (neither is
    a substring of the other): a fixture where the parameter name is a
    substring of the server name (e.g. ``"docs"`` inside ``"docs-server"``)
    would let ``assert parameter in message`` pass on the server's presence
    alone, without the message actually naming the parameter.
    """
    issue = AgentErrorCode.MCP_MARKER_UNKNOWN.issue(
        component="incidents.report",
        parameter="gateway",
        server="docs-server",
        available=["billing-server"],
    )

    assert issue.code is AgentErrorCode.MCP_MARKER_UNKNOWN
    assert issue.component == "incidents.report"
    assert issue.field == "parameters.gateway"
    assert "incidents.report" in issue.message
    assert "gateway" in issue.message
    assert "docs-server" in issue.message
    assert "billing-server" in issue.message


def test_mcp_marker_unknown_declares_none_when_no_servers_are_configured() -> None:
    """An empty deployment still produces a readable message."""
    issue = AgentErrorCode.MCP_MARKER_UNKNOWN.issue(
        component="incidents.report", parameter="docs", server="docs-server", available=[]
    )

    assert "none" in issue.message


def test_agent_marker_output_mismatch_carries_expected_and_declared_types() -> None:
    """Both the annotation's type and the agent's own declared type appear."""
    issue = AgentErrorCode.AGENT_MARKER_OUTPUT_MISMATCH.issue(
        component="incidents.report",
        parameter="triage",
        agent="incident-triage",
        expected="SeverityAssessment",
        declared="TriageVerdict",
    )

    assert issue.code is AgentErrorCode.AGENT_MARKER_OUTPUT_MISMATCH
    assert issue.component == "incidents.report"
    assert issue.field == "parameters.triage"
    assert "SeverityAssessment" in issue.message
    assert "TriageVerdict" in issue.message
    assert "incident-triage" in issue.message


def test_state_declaration_conflict_points_at_deps_type() -> None:
    """Declaring both ``deps_type`` and ``deps_schema`` names the artifact."""
    issue = AgentErrorCode.STATE_DECLARATION_CONFLICT.issue(component="appraisal-bot")

    assert issue.code is AgentErrorCode.STATE_DECLARATION_CONFLICT
    assert issue.component == "appraisal-bot"
    assert issue.field == "deps_type"
    assert "deps_type" in issue.message
    assert "deps_schema" in issue.message
    assert "appraisal-bot" in issue.message


def test_state_type_ref_unresolvable_names_the_reference() -> None:
    """A ``deps_type`` symbol that cannot be imported names the reference."""
    issue = AgentErrorCode.STATE_TYPE_REF_UNRESOLVABLE.issue(
        component="appraisal-bot", ref="myapp.agents:AppraisalDeps"
    )

    assert issue.code is AgentErrorCode.STATE_TYPE_REF_UNRESOLVABLE
    assert issue.component == "appraisal-bot"
    assert issue.field == "deps_type"
    assert "myapp.agents:AppraisalDeps" in issue.message


def test_state_type_ref_unsupported_carries_the_reason() -> None:
    """A resolved symbol msgspec refuses reports the checker's own reason."""
    issue = AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED.issue(
        component="appraisal-bot", ref="myapp.agents:AppraisalDeps", reason="not a msgspec Struct"
    )

    assert issue.code is AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED
    assert issue.component == "appraisal-bot"
    assert issue.field == "deps_type"
    assert "myapp.agents:AppraisalDeps" in issue.message
    assert "not a msgspec Struct" in issue.message


def test_state_type_ref_unresolvable_and_unsupported_are_distinct_codes() -> None:
    """A missing symbol and an unschematisable one are not collapsed."""
    unresolvable = AgentErrorCode.STATE_TYPE_REF_UNRESOLVABLE.issue(
        component="appraisal-bot", ref="myapp.agents:Missing"
    )
    unsupported = AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED.issue(
        component="appraisal-bot", ref="myapp.agents:Present", reason="reason"
    )

    assert unresolvable.code is not unsupported.code


def test_state_schema_invalid_names_the_reason() -> None:
    """An invalid ``deps_schema`` object reports why it is not a JSON Schema."""
    issue = AgentErrorCode.STATE_SCHEMA_INVALID.issue(
        component="appraisal-bot", reason="'type' is not a recognised keyword"
    )

    assert issue.code is AgentErrorCode.STATE_SCHEMA_INVALID
    assert issue.component == "appraisal-bot"
    assert issue.field == "deps_schema"
    assert "'type' is not a recognised keyword" in issue.message


def test_state_schema_invalid_does_not_reuse_output_schema_invalid() -> None:
    """A state-schema fault and an output-schema fault must be filterable apart."""
    state_issue = AgentErrorCode.STATE_SCHEMA_INVALID.issue(
        component="appraisal-bot", reason="reason"
    )
    output_issue = AgentErrorCode.OUTPUT_SCHEMA_INVALID.issue(
        component="appraisal-bot", reason="reason"
    )

    assert state_issue.code is not output_issue.code
    assert state_issue.field != output_issue.field


def test_state_surface_unsupported_names_the_artifact_and_the_surface() -> None:
    """Both halves of the conflict — declared state and the surface — are named."""
    issue = AgentErrorCode.STATE_SURFACE_UNSUPPORTED.issue(component="appraisal-bot", surface="a2a")

    assert issue.code is AgentErrorCode.STATE_SURFACE_UNSUPPORTED
    assert issue.component == "appraisal-bot"
    assert issue.field == "deps_type"
    assert "appraisal-bot" in issue.message
    assert "a2a" in issue.message


def test_instruction_block_invalid_names_the_reason() -> None:
    """A compile-time instruction rule violation carries its own reason."""
    issue = AgentErrorCode.INSTRUCTION_BLOCK_INVALID.issue(
        component="appraisal-bot", reason="duplicate block name 'base' at positions 0 and 2"
    )

    assert issue.code is AgentErrorCode.INSTRUCTION_BLOCK_INVALID
    assert issue.component == "appraisal-bot"
    assert issue.field == "instructions"
    assert "duplicate block name 'base' at positions 0 and 2" in issue.message


def test_template_compilation_failed_names_the_block_and_the_checkers_reason() -> None:
    """The block is named by identity, and the checker's own message is preserved."""
    issue = AgentErrorCode.TEMPLATE_COMPILATION_FAILED.issue(
        component="appraisal-bot", block="context", reason="unknown marker 'motor.ccc' at $.motor"
    )

    assert issue.code is AgentErrorCode.TEMPLATE_COMPILATION_FAILED
    assert issue.component == "appraisal-bot"
    assert issue.field == "instructions"
    assert "context" in issue.message
    assert "unknown marker 'motor.ccc' at $.motor" in issue.message


def test_template_extra_missing_names_the_block_and_the_extra() -> None:
    """The message names both the block that declares ``template:`` and the extra."""
    issue = AgentErrorCode.TEMPLATE_EXTRA_MISSING.issue(
        component="appraisal-bot", block="context", extra="handlebars"
    )

    assert issue.code is AgentErrorCode.TEMPLATE_EXTRA_MISSING
    assert issue.component == "appraisal-bot"
    assert issue.field == "instructions"
    assert "context" in issue.message
    assert "handlebars" in issue.message


def test_state_and_instruction_messages_never_embed_a_url() -> None:
    """None of these eight codes embeds a URL.

    ``STATE_TYPE_REF_UNSUPPORTED``, ``STATE_SCHEMA_INVALID``,
    ``INSTRUCTION_BLOCK_INVALID`` and ``TEMPLATE_COMPILATION_FAILED``
    interpolate a caller-supplied ``reason`` verbatim, and
    ``TEMPLATE_COMPILATION_FAILED``'s ``reason`` is documented as the template
    checker's own message, which can carry a fragment of the source template.
    Redacting that is the caller's responsibility, not this test's.
    """
    issues = [
        AgentErrorCode.STATE_DECLARATION_CONFLICT.issue(component="appraisal-bot"),
        AgentErrorCode.STATE_TYPE_REF_UNRESOLVABLE.issue(
            component="appraisal-bot", ref="myapp.agents:AppraisalDeps"
        ),
        AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED.issue(
            component="appraisal-bot", ref="myapp.agents:AppraisalDeps", reason="reason"
        ),
        AgentErrorCode.STATE_SCHEMA_INVALID.issue(component="appraisal-bot", reason="reason"),
        AgentErrorCode.STATE_SURFACE_UNSUPPORTED.issue(component="appraisal-bot", surface="a2a"),
        AgentErrorCode.INSTRUCTION_BLOCK_INVALID.issue(component="appraisal-bot", reason="reason"),
        AgentErrorCode.TEMPLATE_COMPILATION_FAILED.issue(
            component="appraisal-bot", block="context", reason="reason"
        ),
        AgentErrorCode.TEMPLATE_EXTRA_MISSING.issue(
            component="appraisal-bot", block="context", extra="handlebars"
        ),
    ]

    for issue in issues:
        assert "http://" not in issue.message
        assert "https://" not in issue.message
