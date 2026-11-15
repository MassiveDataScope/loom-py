"""Agent compilation and run-time error codes.

Two disjoint catalogues, mirroring :mod:`loom.streaming.compiler._errors`:

``AgentErrorCode``
    Compile-time failures.  A broken artifact or an unresolvable deployment
    produces one or more :class:`AgentCompilationIssue`, and
    :class:`AgentCompilationError` aggregates them so a single run reports
    every problem at once.

``AgentRunErrorCode``
    Execution outcomes.  Each code belongs to an :class:`AgentRunErrorClass`
    and the retry policy reads the *class*, never the message.

Compile issues
    Each ``AgentErrorCode`` member carries the template of its issue, and
    ``AgentErrorCode.X.issue(...)`` builds it from keyword parameters.
    ``component`` names the artifact component or configuration path the
    issue is attributed to — ``"market"``, ``"ai.mcp_servers.data"``.  Every
    other parameter is interpolated into the human-readable ``message`` and
    is never read for control flow: callers branch on the code, never on the
    text.  Messages never carry secret material — in credential-related
    issues the offending value is deliberately omitted.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum, unique
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, TypeAlias

from loom.core.errors import Forbidden, Unauthenticated
from loom.core.errors.codes import CompileIssue, IssueCode, Template
from loom.core.execution import execution_identity
from loom.core.identity import Acting
from loom.core.sql.abc import RoleNotAllowedError, RolesNotBoundError

if TYPE_CHECKING:
    from loom.ai.abc import AgentUsage
    from loom.core.identity import Identity

_FIELD_SPEC_VERSION: Final = "spec_version"
_FIELD_CAPABILITIES: Final = "capabilities"
_FIELD_CAPABILITIES_KEYS: Final = "capabilities.keys"
_FIELD_FROM_PARAM: Final = "{field}"
_FIELD_CAPABILITIES_CONNECTION: Final = "capabilities.connection"
_FIELD_CAPABILITIES_LIBRARY: Final = "capabilities.library"
_FIELD_CAPABILITIES_FACTORY: Final = "capabilities.factory"
_FIELD_AI_ENGINE: Final = "ai.engine"
_FIELD_AI_SPECS: Final = "ai.specs"
_FIELD_DEPS_TYPE: Final = "deps_type"
_FIELD_INSTRUCTIONS: Final = "instructions"
_ON_OUTPUT_USECASE_FIELD: Final = "on_output.usecase"
_CONVERSATION_USECASE_FIELD: Final = "conversation.usecase"
_AGENT_MARKER_FIELD_TEMPLATE: Final = "parameters.{parameter}"
"""Field of every issue about a use case's ``Agent()`` or ``Mcp()`` marker."""

INVOKER_MISSING_REASON: Final[str] = "no use-case invoker is configured"
"""``reason`` of the invoker-missing issues when the container holds no invoker."""


@unique
class AgentErrorCode(IssueCode):
    """Catalogue of every agent compile-time failure and the issue it renders."""

    SPEC_VERSION_MISSING = (
        "SPEC_VERSION_MISSING",
        Template(
            "{component}: spec_version is missing; it must be declared first",
            field=_FIELD_SPEC_VERSION,
        ),
    )
    SPEC_VERSION_UNSUPPORTED = (
        "SPEC_VERSION_UNSUPPORTED",
        Template(
            "{component}: spec_version {found} is not supported; supported versions: {supported}",
            field=_FIELD_SPEC_VERSION,
        ),
    )
    SPEC_UNKNOWN_FIELD = (
        "SPEC_UNKNOWN_FIELD",
        Template(
            "{component}: unknown field '{field}'; unknown fields are rejected",
            field=_FIELD_FROM_PARAM,
        ),
    )
    SPEC_MALFORMED = ("SPEC_MALFORMED", Template("{component}: malformed artifact: {reason}"))
    AGENT_NAME_INVALID = (
        "AGENT_NAME_INVALID",
        Template("{component}: invalid agent name: {reason}", field="name"),
    )
    AGENT_NAME_DUPLICATE = (
        "AGENT_NAME_DUPLICATE",
        Template("agent '{component}' is declared more than once: {sources}", field="name"),
    )

    OUTPUT_SCHEMA_INVALID = (
        "OUTPUT_SCHEMA_INVALID",
        Template(
            "{component}: output schema is not a valid JSON Schema: {reason}", field="output.schema"
        ),
    )
    OUTPUT_TYPE_REF_UNRESOLVABLE = (
        "OUTPUT_TYPE_REF_UNRESOLVABLE",
        Template(
            "{component}: output type reference '{ref}' cannot be imported", field="output.ref"
        ),
    )
    OUTPUT_TYPE_REF_UNSUPPORTED = (
        "OUTPUT_TYPE_REF_UNSUPPORTED",
        Template(
            "{component}: output type reference '{ref}' is unsupported: {reason}",
            field="output.ref",
        ),
    )
    OUTPUT_CHECK_UNRESOLVABLE = (
        "OUTPUT_CHECK_UNRESOLVABLE",
        Template("{component}: output check '{ref}' cannot be imported", field="output_check"),
    )
    ON_OUTPUT_USECASE_UNKNOWN = (
        "ON_OUTPUT_USECASE_UNKNOWN",
        Template(
            "{component}: on_output use case '{key}' is not registered",
            field=_ON_OUTPUT_USECASE_FIELD,
        ),
    )
    ON_OUTPUT_INPUT_UNSATISFIED = (
        "ON_OUTPUT_INPUT_UNSATISFIED",
        Template(
            "{component}: on_output use case '{key}' cannot be fed from the run: {reason}",
            field=_ON_OUTPUT_USECASE_FIELD,
        ),
    )
    ON_OUTPUT_USECASE_ALSO_GRANTED = (
        "ON_OUTPUT_USECASE_ALSO_GRANTED",
        Template(
            "{component}: on_output use case '{key}' is also granted as a capability; a hook "
            "use case must not be callable by the model",
            field=_ON_OUTPUT_USECASE_FIELD,
        ),
    )
    ON_OUTPUT_INVOKER_MISSING = (
        "ON_OUTPUT_INVOKER_MISSING",
        Template(
            "agents declare an output hook but {reason}: {agents}",
            component="ai",
            field="on_output",
        ),
    )

    USECASE_INVOKER_MISSING = (
        "USECASE_INVOKER_MISSING",
        Template(
            "agents grant use cases but {reason}: {agents}",
            component="ai",
            field=_FIELD_CAPABILITIES,
        ),
    )
    INVOKER_BOUND = (
        "INVOKER_BOUND",
        Template(
            "the container's ApplicationInvoker is bound to an identity; agents need the "
            "unbound invoker so every use case acts as the identity acting",
            component="ai",
        ),
    )

    CONVERSATION_USECASE_UNKNOWN = (
        "CONVERSATION_USECASE_UNKNOWN",
        Template(
            "{component}: conversation use case '{key}' is not registered",
            field=_CONVERSATION_USECASE_FIELD,
        ),
    )
    CONVERSATION_INPUT_UNSATISFIED = (
        "CONVERSATION_INPUT_UNSATISFIED",
        Template(
            "{component}: conversation use case '{key}' cannot be fed from the run: {reason}",
            field=_CONVERSATION_USECASE_FIELD,
        ),
    )
    CONVERSATION_USECASE_ALSO_GRANTED = (
        "CONVERSATION_USECASE_ALSO_GRANTED",
        Template(
            "{component}: conversation use case '{key}' is also granted as a capability; a "
            "loader use case must not be callable by the model",
            field=_CONVERSATION_USECASE_FIELD,
        ),
    )
    CONVERSATION_INVOKER_MISSING = (
        "CONVERSATION_INVOKER_MISSING",
        Template(
            "agents declare a conversation loader but {reason}: {agents}",
            component="ai",
            field="conversation",
        ),
    )

    STATE_DECLARATION_CONFLICT = (
        "STATE_DECLARATION_CONFLICT",
        Template(
            "{component}: both 'deps_type' and 'deps_schema' are declared; declare at most one",
            field=_FIELD_DEPS_TYPE,
        ),
    )
    STATE_TYPE_REF_UNRESOLVABLE = (
        "STATE_TYPE_REF_UNRESOLVABLE",
        Template(
            "{component}: state type reference '{ref}' cannot be imported", field=_FIELD_DEPS_TYPE
        ),
    )
    STATE_TYPE_REF_UNSUPPORTED = (
        "STATE_TYPE_REF_UNSUPPORTED",
        Template(
            "{component}: state type reference '{ref}' is unsupported: {reason}",
            field=_FIELD_DEPS_TYPE,
        ),
    )
    STATE_SCHEMA_INVALID = (
        "STATE_SCHEMA_INVALID",
        Template(
            "{component}: state schema is not a valid JSON Schema: {reason}",
            field="deps_schema",
        ),
    )
    STATE_SURFACE_UNSUPPORTED = (
        "STATE_SURFACE_UNSUPPORTED",
        Template(
            "{component}: declares state and is exposed over '{surface}', which carries no state",
            field=_FIELD_DEPS_TYPE,
        ),
    )

    INSTRUCTION_BLOCK_INVALID = (
        "INSTRUCTION_BLOCK_INVALID",
        Template("{component}: instruction block is invalid: {reason}", field=_FIELD_INSTRUCTIONS),
    )
    TEMPLATE_COMPILATION_FAILED = (
        "TEMPLATE_COMPILATION_FAILED",
        Template(
            "{component}: instruction block '{block}' fails to compile: {reason}",
            field=_FIELD_INSTRUCTIONS,
        ),
    )
    TEMPLATE_EXTRA_MISSING = (
        "TEMPLATE_EXTRA_MISSING",
        Template(
            "{component}: instruction block '{block}' declares 'template: handlebars' but "
            "the '{extra}' extra is not installed",
            field=_FIELD_INSTRUCTIONS,
        ),
    )

    CAPABILITY_KIND_UNSUPPORTED = (
        "CAPABILITY_KIND_UNSUPPORTED",
        Template(
            "{component}: engine '{engine}' does not support capability kind '{kind}'",
            field=_FIELD_CAPABILITIES,
        ),
    )
    STREAMING_REQUIRED_BY_CAPABILITY = (
        "STREAMING_REQUIRED_BY_CAPABILITY",
        Template(
            "{component}: bound with 'streaming: false', but a granted capability requires a "
            "streamed run, so the binding cannot ask for whole answers",
            field="ai.models",
        ),
    )
    NATIVE_TOOL_UNSUPPORTED = (
        "NATIVE_TOOL_UNSUPPORTED",
        Template(
            "{component}: native tool '{tool}' is not supported by the model bound to role "
            "'{role}' (provider '{provider}', model '{model}'); that binding supports: "
            "{supported}",
            field="capabilities.tool",
        ),
    )
    NATIVE_TOOL_DUPLICATE = (
        "NATIVE_TOOL_DUPLICATE",
        Template(
            "{component}: native tool '{tool}' is granted more than once", field="capabilities.tool"
        ),
    )
    CAPABILITY_EMPTY = (
        "CAPABILITY_EMPTY",
        Template("{component}: capability '{kind}' grants no tool", field=_FIELD_CAPABILITIES),
    )
    USECASE_KEY_UNKNOWN = (
        "USECASE_KEY_UNKNOWN",
        Template(
            "{component}: use-case key '{key}' is not registered", field=_FIELD_CAPABILITIES_KEYS
        ),
    )
    USECASE_DECLARES_QUERY = (
        "USECASE_DECLARES_QUERY",
        Template(
            "{component}: use case '{key}' declares a Query() parameter, which an agent tool "
            "call cannot bind; grant a use case without one",
            field=_FIELD_CAPABILITIES_KEYS,
        ),
    )
    SQL_CONNECTION_UNKNOWN = (
        "SQL_CONNECTION_UNKNOWN",
        Template(
            "{component}: sql connection '{connection}' is not configured",
            field=_FIELD_CAPABILITIES_CONNECTION,
        ),
    )
    SQL_CONNECTION_NOT_READONLY = (
        "SQL_CONNECTION_NOT_READONLY",
        Template(
            "{component}: sql connection '{connection}' is not read-only",
            field=_FIELD_CAPABILITIES_CONNECTION,
        ),
    )
    SQL_CONFIG_MISSING = (
        "SQL_CONFIG_MISSING",
        Template(
            "{component}: sql capability declared with no data-layer configuration",
            field=_FIELD_CAPABILITIES_CONNECTION,
        ),
    )
    SQL_CONNECTION_ROLES_UNBOUND = (
        "SQL_CONNECTION_ROLES_UNBOUND",
        Template(
            "{component}: roles of sql connection '{connection}' cannot be bound to a caller "
            "identity",
            field=_FIELD_CAPABILITIES_CONNECTION,
        ),
    )
    SQL_RESULT_BOUND_MISSING = (
        "SQL_RESULT_BOUND_MISSING",
        Template(
            "{component}: sql connection '{connection}' declares no max_rows / "
            "max_result_bytes bound",
            field="capabilities.max_rows",
        ),
    )
    MCP_SERVER_UNKNOWN = (
        "MCP_SERVER_UNKNOWN",
        Template(
            "{component}: mcp server '{server}' is not configured in ai.mcp_servers",
            field="capabilities.server",
        ),
    )
    MCP_URL_INVALID = (
        "MCP_URL_INVALID",
        Template("{component}: invalid mcp url '{url}': {reason}", field="url"),
    )
    MCP_CREDENTIALS_INLINE = (
        "MCP_CREDENTIALS_INLINE",
        Template(
            "{component}: '{field}' carries inline credentials; reference them through "
            "deployment configuration instead",
            field=_FIELD_FROM_PARAM,
        ),
    )
    MCP_HEADERS_REF_INVALID = (
        "MCP_HEADERS_REF_INVALID",
        Template(
            "{component}: 'headers_ref' must resolve to one 'Name=value' header pair; use an "
            "'auth' strategy for anything richer",
            field="headers_ref",
        ),
    )
    MCP_AUTH_CONFLICT = (
        "MCP_AUTH_CONFLICT",
        Template(
            "{component}: 'headers_ref' and 'auth' are mutually exclusive; one connection "
            "carries one credential",
            field="auth",
        ),
    )
    MCP_AUTH_STRATEGY_UNKNOWN = (
        "MCP_AUTH_STRATEGY_UNKNOWN",
        Template(
            "{component}: auth strategy '{kind}' is not registered in entry-point group "
            "'loom.ai.remote_auth'; registered: {available}",
            field="auth.kind",
        ),
    )
    MCP_AUTH_STRATEGY_INVALID = (
        "MCP_AUTH_STRATEGY_INVALID",
        Template(
            "auth strategy '{kind}' is unusable: {reason}",
            component="loom.ai.remote_auth:{kind}",
            field="auth.kind",
        ),
    )
    MCP_TRANSPORT_INVALID = (
        "MCP_TRANSPORT_INVALID",
        Template("{component}: invalid mcp transport: {reason}", field=_FIELD_FROM_PARAM),
    )
    SKILLS_LIBRARY_INVALID = (
        "SKILLS_LIBRARY_INVALID",
        Template(
            "{component}: skills library '{library}' is unusable: {reason}",
            field=_FIELD_CAPABILITIES_LIBRARY,
        ),
    )
    SKILLS_LIBRARY_ESCAPES = (
        "SKILLS_LIBRARY_ESCAPES",
        Template(
            "{component}: skills library '{library}' escapes its own directory",
            field=_FIELD_CAPABILITIES_LIBRARY,
        ),
    )
    SKILLS_NAME_COLLISION = (
        "SKILLS_NAME_COLLISION",
        Template(
            "{component}: skill '{skill}' is exposed by both libraries '{first_library}' and "
            "'{second_library}'",
            field=_FIELD_CAPABILITIES_LIBRARY,
        ),
    )
    SKILLS_ROOT_MISSING = (
        "SKILLS_ROOT_MISSING",
        Template(
            "{component}: a bare skills library requires a configured skills_root; use "
            "'./name' to resolve it beside the artifact instead",
            field=_FIELD_CAPABILITIES_LIBRARY,
        ),
    )
    PYTHON_FACTORY_UNRESOLVABLE = (
        "PYTHON_FACTORY_UNRESOLVABLE",
        Template(
            "{component}: python factory '{factory}' cannot be imported: {reason}",
            field=_FIELD_CAPABILITIES_FACTORY,
        ),
    )
    PYTHON_FACTORY_NOT_CALLABLE = (
        "PYTHON_FACTORY_NOT_CALLABLE",
        Template(
            "{component}: python factory '{factory}' does not satisfy ToolsetFactory",
            field=_FIELD_CAPABILITIES_FACTORY,
        ),
    )
    PYTHON_FACTORY_PARAMS_REJECTED = (
        "PYTHON_FACTORY_PARAMS_REJECTED",
        Template(
            "{component}: python factory '{factory}' rejects params: {reason}",
            field="capabilities.params",
        ),
    )
    PYTHON_REMOTE_NOT_GRANTED = (
        "PYTHON_REMOTE_NOT_GRANTED",
        Template(
            "{component}: python factory '{factory}' asked for mcp server '{server}', but "
            "agent '{component}' has no mcp grant on that server",
            field=_FIELD_CAPABILITIES_FACTORY,
        ),
    )
    PYTHON_FACTORY_FAILED = (
        "PYTHON_FACTORY_FAILED",
        Template(
            "{component}: python factory '{factory}' raised {error} while building",
            field=_FIELD_CAPABILITIES_FACTORY,
        ),
    )
    A2A_AGENT_UNKNOWN = (
        "A2A_AGENT_UNKNOWN",
        Template(
            "{component}: a2a agent '{agent}' is not configured in ai.a2a_agents",
            field="capabilities.agent",
        ),
    )
    A2A_URL_INVALID = (
        "A2A_URL_INVALID",
        Template("{component}: invalid a2a url '{url}': {reason}", field="url"),
    )
    ANONYMOUS_WITH_DATA_CAPABILITY = (
        "ANONYMOUS_WITH_DATA_CAPABILITY",
        Template(
            "{component}: agent opts out of authentication while holding the '{kind}' capability",
            field=_FIELD_CAPABILITIES,
        ),
    )

    MODEL_ROLE_UNBOUND = (
        "MODEL_ROLE_UNBOUND",
        Template("{component}: model role '{role}' is not bound in ai.models", field="model_role"),
    )
    INFERENCE_TARGET_INCOMPLETE = (
        "INFERENCE_TARGET_INCOMPLETE",
        Template(
            "model role '{role}': required setting '{setting}' is missing",
            component="model role '{role}'",
            field="{setting}",
        ),
    )
    OUTPUT_MODE_UNKNOWN = (
        "OUTPUT_MODE_UNKNOWN",
        Template(
            "model role '{role}': output_mode '{value}' is not one of {valid}",
            component="model role '{role}'",
            field="output_mode",
        ),
    )
    OUTPUT_MODE_UNSUPPORTED = (
        "OUTPUT_MODE_UNSUPPORTED",
        Template(
            "provider '{component}': output_mode '{mode}' is not served by its models; "
            "supported modes: {supported}",
            field="output_mode",
        ),
    )
    POLICY_OUT_OF_RANGE = (
        "POLICY_OUT_OF_RANGE",
        Template(
            "{component}: policy '{policy}' value {value} is outside the allowed range "
            "{minimum}..{maximum}",
            field="policies.{policy}",
        ),
    )

    ENGINE_NOT_FOUND = (
        "ENGINE_NOT_FOUND",
        Template(
            "engine '{component}' is not installed; available engines: {available}",
            field=_FIELD_AI_ENGINE,
        ),
    )
    ENGINE_DUPLICATE = (
        "ENGINE_DUPLICATE",
        Template(
            "engine '{component}' is provided by more than one distribution: {distributions}",
            field=_FIELD_AI_ENGINE,
        ),
    )
    ENGINE_API_MISMATCH = (
        "ENGINE_API_MISMATCH",
        Template(
            "engine '{component}' speaks handshake version {found}; "
            "supported versions: {supported}",
            field=_FIELD_AI_ENGINE,
        ),
    )
    PROVIDER_NOT_INSTALLED = (
        "PROVIDER_NOT_INSTALLED",
        Template(
            "provider '{component}' is not installed; install the '{extra}' extra",
        ),
    )
    PROVIDER_UNKNOWN = (
        "PROVIDER_UNKNOWN",
        Template(
            "provider '{component}' is not known to this release of loom; supported "
            "providers: {supported}",
        ),
    )
    PROVIDER_SETTING_MISSING = (
        "PROVIDER_SETTING_MISSING",
        Template(
            "provider '{component}': required setting '{setting}' is missing",
            field="{setting}",
        ),
    )
    MCP_SERVER_UNREACHABLE = (
        "MCP_SERVER_UNREACHABLE",
        Template("mcp server '{server}' is unreachable: {reason}", component="{server}"),
    )
    MCP_CONNECTION_CONFLICT = (
        "MCP_CONNECTION_CONFLICT",
        Template(
            "mcp server '{server}' resolves to different connections for agents {agents}: "
            "one worker opens a single client per server, so the transport, address, "
            "credential and deadline of every grant of that name must be identical",
            component="{server}",
            field="capabilities.server",
        ),
    )
    TOOL_FILTER_MATCHES_NOTHING = (
        "TOOL_FILTER_MATCHES_NOTHING",
        Template(
            "{component}: filter for '{target}' matches no tool", field="capabilities.include"
        ),
    )
    SQL_READONLY_DRIFT = (
        "SQL_READONLY_DRIFT",
        Template(
            "sql connection '{connection}' is no longer read-only; the compiled plan assumed "
            "it was",
            component="{connection}",
        ),
    )
    ENDPOINT_AUTH_MISSING = (
        "ENDPOINT_AUTH_MISSING",
        Template("{component}: HTTP exposure requires a named authentication", field="auth"),
    )
    A2A_BASE_URL_INVALID = (
        "A2A_BASE_URL_INVALID",
        Template(
            "the published card base URL is unsafe: {reason} ({url})",
            component="ai.a2a",
            field="base_url",
        ),
    )
    A2A_EXPOSE_EMPTY = (
        "A2A_EXPOSE_EMPTY",
        Template("a2a exposure is enabled but names no agent", component="a2a", field="a2a.expose"),
    )
    AUTH_EXCLUSION_OVERLAPS_AGENTS = (
        "AUTH_EXCLUSION_OVERLAPS_AGENTS",
        Template(
            "authentication exclusions cover agent invocation paths: {paths}",
            component="auth",
            field="auth.exclude",
        ),
    )
    A2A_AGENT_UNREACHABLE = (
        "A2A_AGENT_UNREACHABLE",
        Template("remote a2a agent '{agent}' is unreachable: {reason}", component="{agent}"),
    )
    AGENT_SPECS_CONFLICT = (
        "AGENT_SPECS_CONFLICT",
        Template(
            "agent artifacts are declared both by the manifest 'AGENTS' attribute and by the "
            "'ai.specs' config key; declare them in exactly one of the two",
            component="ai",
            field=_FIELD_AI_SPECS,
        ),
    )
    AGENT_SPECS_MISSING = (
        "AGENT_SPECS_MISSING",
        Template(
            "the 'ai:' section is configured but declares no agent artifact; set 'ai.specs' "
            "or the manifest 'AGENTS' attribute",
            component="ai",
            field=_FIELD_AI_SPECS,
        ),
    )
    AGENT_SPECS_ESCAPE_ROOT = (
        "AGENT_SPECS_ESCAPE_ROOT",
        Template(
            "agent artifact pattern '{pattern}' reaches outside the directory it is resolved "
            "against; write it relative to that directory, without '..'",
            component="ai",
            field=_FIELD_AI_SPECS,
        ),
    )
    REMOTE_CLIENTS_UNKNOWN = (
        "REMOTE_CLIENTS_UNKNOWN",
        Template(
            "ai.remote_clients: '{value}' is not a known start-up mode for remote clients; "
            "accepted: {valid}",
            component="ai",
            field="ai.remote_clients",
        ),
    )
    MAX_AGENT_DEPTH_INVALID = (
        "MAX_AGENT_DEPTH_INVALID",
        Template(
            "ai.max_agent_depth: {value} is below the minimum of 1; the top-level run itself "
            "counts as one entry in the chain, so a value below 1 refuses every run, "
            "including the top-level one",
            component="ai",
            field="ai.max_agent_depth",
        ),
    )
    MAX_CONCURRENT_RUNS_INVALID = (
        "MAX_CONCURRENT_RUNS_INVALID",
        Template(
            "ai.max_concurrent_runs: {value} is below the minimum of 1; a worker with no run "
            "slot refuses every run and an ETL batch answers no row",
            component="ai",
            field="ai.max_concurrent_runs",
        ),
    )

    AGENT_MARKER_UNKNOWN = (
        "AGENT_MARKER_UNKNOWN",
        Template(
            "{component}: parameter '{parameter}' names unknown agent '{agent}'; compiled "
            "agents: {available}",
            field=_AGENT_MARKER_FIELD_TEMPLATE,
        ),
    )
    AGENT_MARKER_OUTPUT_MISMATCH = (
        "AGENT_MARKER_OUTPUT_MISMATCH",
        Template(
            "{component}: parameter '{parameter}' declares AgentHandle[{expected}] for agent "
            "'{agent}', but '{agent}' declares output '{declared}'",
            field=_AGENT_MARKER_FIELD_TEMPLATE,
        ),
    )

    USECASE_TOOL_FILTER_MATCHES_NOTHING = (
        "USECASE_TOOL_FILTER_MATCHES_NOTHING",
        Template(
            "{component}: parameter '{parameter}' declares an include filter for mcp server "
            "'{server}' that matches no tool",
            field=_AGENT_MARKER_FIELD_TEMPLATE,
        ),
    )
    MCP_MARKER_UNKNOWN = (
        "MCP_MARKER_UNKNOWN",
        Template(
            "{component}: parameter '{parameter}' names unknown mcp server '{server}'; "
            "configured servers: {available}",
            field=_AGENT_MARKER_FIELD_TEMPLATE,
        ),
    )

    TOOL_NAME_COLLISION = (
        "TOOL_NAME_COLLISION",
        Template(
            "{component}: grants '{first}' and '{second}' both derive the tool name '{name}'",
            field=_FIELD_CAPABILITIES,
        ),
    )
    TOOL_NAME_TOO_LONG = (
        "TOOL_NAME_TOO_LONG",
        Template(
            "{component}: grant '{granted}' derives the tool name '{name}' of {length} "
            "characters, above the {limit}-character bound providers accept",
            field=_FIELD_CAPABILITIES,
        ),
    )
    USECASE_NOT_COMPILED = (
        "USECASE_NOT_COMPILED",
        Template(
            "use case '{key}' carries no compiled execution plan; it was never compiled",
            field=_FIELD_CAPABILITIES_KEYS,
            component="use case '{key}'",
        ),
    )
    CAPABILITY_KIND_NO_BUILDER = (
        "CAPABILITY_KIND_NO_BUILDER",
        Template("{component}: capability kind '{kind}' has no builder", field=_FIELD_CAPABILITIES),
    )
    MCP_CONNECTION_NOT_OPENED = (
        "MCP_CONNECTION_NOT_OPENED",
        Template(
            "{component}: the runtime never opened mcp server '{server}' through this "
            "engine's shared toolsets. The runtime's 'mcp_client_factory' must be the "
            "'mcp_client_factory' of the very provider instance that builds the engines, "
            "or the run would hold a second connection whose tool filters start-up never "
            "validated",
            field=_FIELD_CAPABILITIES_CONNECTION,
        ),
    )

    SPEC_VERSION_DEPRECATED = (
        "SPEC_VERSION_DEPRECATED",
        Template(
            "{component}: spec_version {found} is deprecated; version {latest} is the current one",
            field=_FIELD_SPEC_VERSION,
        ),
    )


AgentCompilationIssue: TypeAlias = CompileIssue[AgentErrorCode]


class AgentCompilationError(Exception):
    """Raised when one or more agent artifacts fail to compile.

    Aggregates every :class:`AgentCompilationIssue` found in a compilation run
    so a generator sees the whole picture instead of one failure at a time.

    Attributes:
        issues: Structured issues, one per failure.

    Args:
        issues: Issues collected by the compiler.
    """

    def __init__(self, issues: Sequence[AgentCompilationIssue]) -> None:
        self.issues: tuple[AgentCompilationIssue, ...] = tuple(issues)
        messages = [issue.message for issue in self.issues]
        super().__init__(
            f"Agent compilation failed with {len(messages)} error(s): {'; '.join(messages)}"
        )


class AgentRunErrorClass(StrEnum):
    """Class of a run-time failure; the retry policy reads this, not the message."""

    INFRASTRUCTURE = "INFRASTRUCTURE"
    MODEL_BEHAVIOUR = "MODEL_BEHAVIOUR"
    LIMIT = "LIMIT"
    AUTHORIZATION = "AUTHORIZATION"
    CLIENT = "CLIENT"
    APPLICATION = "APPLICATION"


class AgentRunErrorCode(StrEnum):
    """Enumeration of all agent run-time failure codes."""

    PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE"
    PROVIDER_RATE_LIMITED = "PROVIDER_RATE_LIMITED"
    TOOL_TIMEOUT = "TOOL_TIMEOUT"
    TOOL_UNAVAILABLE = "TOOL_UNAVAILABLE"
    OUTPUT_SCHEMA_VIOLATION = "OUTPUT_SCHEMA_VIOLATION"
    MAX_ITERATIONS_EXCEEDED = "MAX_ITERATIONS_EXCEEDED"
    USAGE_LIMIT_EXCEEDED = "USAGE_LIMIT_EXCEEDED"
    COST_NOT_MEASURABLE = "COST_NOT_MEASURABLE"
    RUN_TIMEOUT = "RUN_TIMEOUT"
    TOO_MANY_RUNS = "TOO_MANY_RUNS"
    UNAUTHORIZED = "UNAUTHORIZED"
    CANCELLED = "CANCELLED"
    HOOK_FAILED = "HOOK_FAILED"
    CONVERSATION_LOAD_FAILED = "CONVERSATION_LOAD_FAILED"
    CONVERSATION_LOAD_TIMEOUT = "CONVERSATION_LOAD_TIMEOUT"
    STATE_UNDECLARED = "STATE_UNDECLARED"
    STATE_REQUIRED = "STATE_REQUIRED"

    MCP_GRANT_UNKNOWN = "MCP_GRANT_UNKNOWN"
    MCP_SERVER_NOT_GRANTED = "MCP_SERVER_NOT_GRANTED"
    SQL_GRANT_UNKNOWN = "SQL_GRANT_UNKNOWN"
    TOOL_UNKNOWN = "TOOL_UNKNOWN"
    TOOL_UNTYPED = "TOOL_UNTYPED"
    TOOL_RESULT_UNSTRUCTURED = "TOOL_RESULT_UNSTRUCTURED"
    TOOL_DECODE_FAILED = "TOOL_DECODE_FAILED"
    TOOL_CALL_FAILED = "TOOL_CALL_FAILED"
    MCP_CALL_INTERRUPTED = "MCP_CALL_INTERRUPTED"
    AGENT_CALL_CYCLE = "AGENT_CALL_CYCLE"
    AGENT_CALL_TOO_DEEP = "AGENT_CALL_TOO_DEEP"
    AGENT_RUN_SHAPE_WITH_HOOK = "AGENT_RUN_SHAPE_WITH_HOOK"


class AgentRunError(Exception):
    """A run failed with a stable, machine-readable code.

    Lives with :class:`AgentRunErrorCode` rather than with the runtime that
    raises it: the engine adapters classify and re-raise it, and importing the
    whole live runtime — its exit stack, its shared sessions, its SQL
    configuration — to reach one exception class would point the dependency
    arrow at the concretion instead of at the contract.

    Args:
        code: Run-time failure code; the retry policy reads its class.
        message: Human-readable description, safe to return to the caller.
        interaction_id: Identifier of the admitted run, when the failure
            happened after admission; ``None`` for pre-admission failures.
        usage: What the failed run had already spent, when the engine knew it;
            ``None`` when nothing was spent or nothing was measurable.

    Attributes:
        code: The failure code carried by this error.
        interaction_id: The run this error belongs to, or ``None``.
        usage: The partial accounting of the failed run, or ``None``.

    Example::

        raise AgentRunError(AgentRunErrorCode.RUN_TIMEOUT, "the run took too long")
    """

    def __init__(
        self,
        code: AgentRunErrorCode,
        message: str,
        *,
        interaction_id: str | None = None,
        usage: AgentUsage | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.interaction_id = interaction_id
        self.usage = usage


_RUN_ERROR_CLASSES: Mapping[AgentRunErrorCode, AgentRunErrorClass] = MappingProxyType(
    {
        AgentRunErrorCode.PROVIDER_UNAVAILABLE: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.PROVIDER_RATE_LIMITED: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.TOOL_TIMEOUT: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.TOOL_UNAVAILABLE: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION: AgentRunErrorClass.MODEL_BEHAVIOUR,
        AgentRunErrorCode.MAX_ITERATIONS_EXCEEDED: AgentRunErrorClass.LIMIT,
        AgentRunErrorCode.USAGE_LIMIT_EXCEEDED: AgentRunErrorClass.LIMIT,
        AgentRunErrorCode.COST_NOT_MEASURABLE: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.RUN_TIMEOUT: AgentRunErrorClass.LIMIT,
        AgentRunErrorCode.TOO_MANY_RUNS: AgentRunErrorClass.LIMIT,
        AgentRunErrorCode.UNAUTHORIZED: AgentRunErrorClass.AUTHORIZATION,
        AgentRunErrorCode.CANCELLED: AgentRunErrorClass.CLIENT,
        AgentRunErrorCode.HOOK_FAILED: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.CONVERSATION_LOAD_FAILED: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.CONVERSATION_LOAD_TIMEOUT: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.STATE_UNDECLARED: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.STATE_REQUIRED: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.MCP_GRANT_UNKNOWN: AgentRunErrorClass.AUTHORIZATION,
        AgentRunErrorCode.MCP_SERVER_NOT_GRANTED: AgentRunErrorClass.AUTHORIZATION,
        AgentRunErrorCode.SQL_GRANT_UNKNOWN: AgentRunErrorClass.AUTHORIZATION,
        AgentRunErrorCode.TOOL_UNKNOWN: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.TOOL_UNTYPED: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.TOOL_DECODE_FAILED: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.TOOL_CALL_FAILED: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.MCP_CALL_INTERRUPTED: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.AGENT_CALL_CYCLE: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.AGENT_CALL_TOO_DEEP: AgentRunErrorClass.LIMIT,
        AgentRunErrorCode.AGENT_RUN_SHAPE_WITH_HOOK: AgentRunErrorClass.APPLICATION,
    }
)

CONVERSATION_LOAD_FAILED_MESSAGE: Final[str] = (
    "the conversation could not be loaded; the detail is recorded server-side"
)
"""Client text of every ``CONVERSATION_LOAD_FAILED`` error, the runtime loader's
and an engine's history decoder's alike; the loader's own detail is logged."""

CONVERSATION_LOAD_TIMEOUT_MESSAGE: Final[str] = "the conversation loader exceeded its time limit"
"""Client text of every ``CONVERSATION_LOAD_TIMEOUT`` error."""

HOOK_FAILED_MESSAGE: Final[str] = "the output hook failed; the detail is recorded server-side"
"""Client text of every ``HOOK_FAILED`` error."""

NO_TERMINAL_MESSAGE: Final[str] = "agent {agent!r} produced no terminal event"
"""Client text of the ``PROVIDER_UNAVAILABLE`` error closing a stream with no terminal event."""

DENIED_MESSAGE: Final[str] = "the caller is not allowed to perform this operation"
"""Client text of every ``UNAUTHORIZED`` error mapped from an application denial."""

DENIALS: Final[tuple[type[Exception], ...]] = (
    Forbidden,
    Unauthenticated,
    RoleNotAllowedError,
    RolesNotBoundError,
)
"""Application denials mapped to ``UNAUTHORIZED``."""


def run_error_class(code: AgentRunErrorCode) -> AgentRunErrorClass:
    """Return the failure class of a run-time error code.

    The mapping is total: every member of :class:`AgentRunErrorCode` has an
    entry, so a new code without a class fails immediately instead of silently
    defaulting to a retriable class.

    Args:
        code: Run-time error code to classify.

    Returns:
        The class the retry policy must read.

    Raises:
        KeyError: If the code has no registered class.
    """
    return _RUN_ERROR_CLASSES[code]


_NEVER_RETRIED: frozenset[AgentRunErrorCode] = frozenset(
    {AgentRunErrorCode.COST_NOT_MEASURABLE, AgentRunErrorCode.MCP_CALL_INTERRUPTED}
)
"""``INFRASTRUCTURE``-classed codes that retrying can never help, or could harm.

An explicit exception list, not a change to :data:`AgentRunErrorClass`:
``COST_NOT_MEASURABLE`` stays ``INFRASTRUCTURE`` (mapped to HTTP 500, not
503, so callers and gateways do not retry it without consulting
:func:`is_retriable`) but is raised only after the run's provider call has
already returned and already been billed, so retrying would only spend the
caller's money again on a gap it does not control. ``MCP_CALL_INTERRUPTED``
is the same carve-out for a different reason: the MCP call it reports may
already have run on the server, so a retried run could run it twice."""


def is_retriable(code: AgentRunErrorCode) -> bool:
    """Return whether a run-time failure may be retried by the caller.

    Only :data:`AgentRunErrorClass.INFRASTRUCTURE` failures are retriable,
    with the explicit exceptions of :data:`_NEVER_RETRIED`:
    ``COST_NOT_MEASURABLE`` (the provider call has already been billed) and
    ``MCP_CALL_INTERRUPTED`` (the MCP call may already have run).

    Args:
        code: Run-time error code to test.

    Returns:
        ``True`` when the code's class is ``INFRASTRUCTURE``; ``False`` for
        every other class, and for the codes of :data:`_NEVER_RETRIED`
        despite their ``INFRASTRUCTURE`` class.
    """
    if code in _NEVER_RETRIED:
        return False
    return run_error_class(code) is AgentRunErrorClass.INFRASTRUCTURE


def require_authenticated(who: str) -> Identity:
    """Return the identity of the execution in progress, refused past its deadline or anonymous.

    Args:
        who: What refuses it, as the refusal names it (``"tool 'x'"``).

    Returns:
        The authenticated identity acting.

    Raises:
        ExecutionRequired: Outside any execution.
        Unauthenticated: When the execution acts with no caller.
        DelegationExpired: Past the caller's deadline, inside a live execution
            or from a task whose execution ended.
        AgentRunError: With ``UNAUTHORIZED`` when the identity acting is not
            authenticated.
    """
    identity = execution_identity()
    Acting.current().refuse_expired()
    if not identity.is_authenticated:
        raise AgentRunError(
            AgentRunErrorCode.UNAUTHORIZED, f"{who} requires an authenticated caller"
        )
    return identity
