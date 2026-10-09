"""Agent compilation and run-time error codes.

Two disjoint catalogues, mirroring :mod:`loom.streaming.compiler._errors`:

``AgentErrorCode``
    Compile-time failures.  A broken artifact or an unresolvable deployment
    produces one or more :class:`AgentCompilationIssue`, and
    :class:`AgentCompilationError` aggregates them so a single run reports
    every problem at once.

``AgentRunErrorCode``
    Execution outcomes.  Each code belongs to an :class:`AgentRunErrorClass`
    and the retry policy reads the *class*, never the message (FR-028).

Every code has a dedicated factory function so call-sites stay
intention-revealing and free of string formatting.

Issue factories
    The factories below share one contract, stated here once rather than
    repeated on each of them.  Every factory returns a single
    :class:`AgentCompilationIssue` carrying its own :class:`AgentErrorCode`.
    ``component`` names the artifact component or configuration path the
    issue is attributed to — ``"market"``, ``"ai.mcp_servers.data"``.  Every
    other parameter is interpolated into the human-readable ``message`` and
    is never read for control flow: callers branch on the code, never on the
    text.  Messages never carry secret material — in credential-related
    issues the offending value is deliberately omitted.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from loom.core.model import LoomFrozenStruct

if TYPE_CHECKING:
    # Type-only: ``loom.ai.abc`` imports this module for the code enum, so a
    # run-time import here would close the cycle. Nothing below needs the
    # class at run time — the error only carries the value it is handed.
    from loom.ai.abc import AgentUsage

# Repeated ``field`` values for issue factories that report on the same
# artifact path from more than one code.
_FIELD_CAPABILITIES_CONNECTION: Final = "capabilities.connection"
_FIELD_CAPABILITIES_LIBRARY: Final = "capabilities.library"
_FIELD_CAPABILITIES_FACTORY: Final = "capabilities.factory"
_FIELD_AI_ENGINE: Final = "ai.engine"
_FIELD_DEPS_TYPE: Final = "deps_type"
_FIELD_DEPS_SCHEMA: Final = "deps_schema"
_FIELD_INSTRUCTIONS: Final = "instructions"


class AgentErrorCode(StrEnum):
    """Enumeration of all agent compile-time failure codes."""

    # Envelope and structure
    SPEC_VERSION_MISSING = "SPEC_VERSION_MISSING"
    SPEC_VERSION_UNSUPPORTED = "SPEC_VERSION_UNSUPPORTED"
    SPEC_UNKNOWN_FIELD = "SPEC_UNKNOWN_FIELD"
    SPEC_MALFORMED = "SPEC_MALFORMED"
    AGENT_NAME_INVALID = "AGENT_NAME_INVALID"
    AGENT_NAME_DUPLICATE = "AGENT_NAME_DUPLICATE"

    # Output
    OUTPUT_SCHEMA_INVALID = "OUTPUT_SCHEMA_INVALID"
    OUTPUT_TYPE_REF_UNRESOLVABLE = "OUTPUT_TYPE_REF_UNRESOLVABLE"
    OUTPUT_TYPE_REF_UNSUPPORTED = "OUTPUT_TYPE_REF_UNSUPPORTED"
    OUTPUT_CHECK_UNRESOLVABLE = "OUTPUT_CHECK_UNRESOLVABLE"
    ON_OUTPUT_USECASE_UNKNOWN = "ON_OUTPUT_USECASE_UNKNOWN"
    ON_OUTPUT_INPUT_UNSATISFIED = "ON_OUTPUT_INPUT_UNSATISFIED"
    ON_OUTPUT_USECASE_ALSO_GRANTED = "ON_OUTPUT_USECASE_ALSO_GRANTED"
    ON_OUTPUT_INVOKER_MISSING = "ON_OUTPUT_INVOKER_MISSING"

    # Conversation
    CONVERSATION_USECASE_UNKNOWN = "CONVERSATION_USECASE_UNKNOWN"
    CONVERSATION_INPUT_UNSATISFIED = "CONVERSATION_INPUT_UNSATISFIED"
    CONVERSATION_USECASE_ALSO_GRANTED = "CONVERSATION_USECASE_ALSO_GRANTED"
    CONVERSATION_INVOKER_MISSING = "CONVERSATION_INVOKER_MISSING"

    # State
    STATE_DECLARATION_CONFLICT = "STATE_DECLARATION_CONFLICT"
    STATE_TYPE_REF_UNRESOLVABLE = "STATE_TYPE_REF_UNRESOLVABLE"
    STATE_TYPE_REF_UNSUPPORTED = "STATE_TYPE_REF_UNSUPPORTED"
    STATE_SCHEMA_INVALID = "STATE_SCHEMA_INVALID"
    STATE_SURFACE_UNSUPPORTED = "STATE_SURFACE_UNSUPPORTED"

    # Instructions
    INSTRUCTION_BLOCK_INVALID = "INSTRUCTION_BLOCK_INVALID"
    TEMPLATE_COMPILATION_FAILED = "TEMPLATE_COMPILATION_FAILED"
    TEMPLATE_EXTRA_MISSING = "TEMPLATE_EXTRA_MISSING"

    # Capabilities
    CAPABILITY_KIND_UNSUPPORTED = "CAPABILITY_KIND_UNSUPPORTED"
    STREAMING_REQUIRED_BY_CAPABILITY = "STREAMING_REQUIRED_BY_CAPABILITY"
    NATIVE_TOOL_UNSUPPORTED = "NATIVE_TOOL_UNSUPPORTED"
    NATIVE_TOOL_DUPLICATE = "NATIVE_TOOL_DUPLICATE"
    CAPABILITY_EMPTY = "CAPABILITY_EMPTY"
    USECASE_KEY_UNKNOWN = "USECASE_KEY_UNKNOWN"
    SQL_CONNECTION_UNKNOWN = "SQL_CONNECTION_UNKNOWN"
    SQL_CONNECTION_NOT_READONLY = "SQL_CONNECTION_NOT_READONLY"
    SQL_CONFIG_MISSING = "SQL_CONFIG_MISSING"
    SQL_CONNECTION_ROLES_UNBOUND = "SQL_CONNECTION_ROLES_UNBOUND"
    SQL_RESULT_BOUND_MISSING = "SQL_RESULT_BOUND_MISSING"
    MCP_SERVER_UNKNOWN = "MCP_SERVER_UNKNOWN"
    MCP_URL_INVALID = "MCP_URL_INVALID"
    MCP_CREDENTIALS_INLINE = "MCP_CREDENTIALS_INLINE"
    MCP_HEADERS_REF_INVALID = "MCP_HEADERS_REF_INVALID"
    MCP_AUTH_CONFLICT = "MCP_AUTH_CONFLICT"
    MCP_AUTH_STRATEGY_UNKNOWN = "MCP_AUTH_STRATEGY_UNKNOWN"
    MCP_AUTH_STRATEGY_INVALID = "MCP_AUTH_STRATEGY_INVALID"
    MCP_TRANSPORT_INVALID = "MCP_TRANSPORT_INVALID"
    SKILLS_LIBRARY_INVALID = "SKILLS_LIBRARY_INVALID"
    SKILLS_LIBRARY_ESCAPES = "SKILLS_LIBRARY_ESCAPES"
    SKILLS_NAME_COLLISION = "SKILLS_NAME_COLLISION"
    SKILLS_ROOT_MISSING = "SKILLS_ROOT_MISSING"
    PYTHON_FACTORY_UNRESOLVABLE = "PYTHON_FACTORY_UNRESOLVABLE"
    PYTHON_FACTORY_NOT_CALLABLE = "PYTHON_FACTORY_NOT_CALLABLE"
    PYTHON_FACTORY_PARAMS_REJECTED = "PYTHON_FACTORY_PARAMS_REJECTED"
    PYTHON_REMOTE_NOT_GRANTED = "PYTHON_REMOTE_NOT_GRANTED"
    PYTHON_FACTORY_FAILED = "PYTHON_FACTORY_FAILED"
    A2A_AGENT_UNKNOWN = "A2A_AGENT_UNKNOWN"
    A2A_URL_INVALID = "A2A_URL_INVALID"
    ANONYMOUS_WITH_DATA_CAPABILITY = "ANONYMOUS_WITH_DATA_CAPABILITY"

    # Model and policy
    MODEL_ROLE_UNBOUND = "MODEL_ROLE_UNBOUND"
    INFERENCE_TARGET_INCOMPLETE = "INFERENCE_TARGET_INCOMPLETE"
    OUTPUT_MODE_UNKNOWN = "OUTPUT_MODE_UNKNOWN"
    OUTPUT_MODE_UNSUPPORTED = "OUTPUT_MODE_UNSUPPORTED"
    POLICY_OUT_OF_RANGE = "POLICY_OUT_OF_RANGE"

    # Deployment resolution
    ENGINE_NOT_FOUND = "ENGINE_NOT_FOUND"
    ENGINE_DUPLICATE = "ENGINE_DUPLICATE"
    ENGINE_API_MISMATCH = "ENGINE_API_MISMATCH"
    PROVIDER_NOT_INSTALLED = "PROVIDER_NOT_INSTALLED"
    PROVIDER_UNKNOWN = "PROVIDER_UNKNOWN"
    PROVIDER_SETTING_MISSING = "PROVIDER_SETTING_MISSING"
    MCP_SERVER_UNREACHABLE = "MCP_SERVER_UNREACHABLE"
    MCP_CONNECTION_CONFLICT = "MCP_CONNECTION_CONFLICT"
    TOOL_FILTER_MATCHES_NOTHING = "TOOL_FILTER_MATCHES_NOTHING"
    SQL_READONLY_DRIFT = "SQL_READONLY_DRIFT"
    ENDPOINT_AUTH_MISSING = "ENDPOINT_AUTH_MISSING"
    A2A_BASE_URL_INVALID = "A2A_BASE_URL_INVALID"
    A2A_EXPOSE_EMPTY = "A2A_EXPOSE_EMPTY"
    AUTH_EXCLUSION_OVERLAPS_AGENTS = "AUTH_EXCLUSION_OVERLAPS_AGENTS"
    A2A_AGENT_UNREACHABLE = "A2A_AGENT_UNREACHABLE"
    AGENT_SPECS_CONFLICT = "AGENT_SPECS_CONFLICT"
    AGENT_SPECS_MISSING = "AGENT_SPECS_MISSING"
    AGENT_SPECS_ESCAPE_ROOT = "AGENT_SPECS_ESCAPE_ROOT"
    REMOTE_CLIENTS_UNKNOWN = "REMOTE_CLIENTS_UNKNOWN"
    MAX_AGENT_DEPTH_INVALID = "MAX_AGENT_DEPTH_INVALID"

    # Use-case agent markers (model-as-actor)
    AGENT_MARKER_UNKNOWN = "AGENT_MARKER_UNKNOWN"
    AGENT_MARKER_OUTPUT_MISMATCH = "AGENT_MARKER_OUTPUT_MISMATCH"

    # Use-case MCP markers
    MCP_MARKER_UNKNOWN = "MCP_MARKER_UNKNOWN"

    # Compatibility
    SPEC_VERSION_DEPRECATED = "SPEC_VERSION_DEPRECATED"
    UNSPECIFIED = "UNSPECIFIED"


class AgentCompilationIssue(LoomFrozenStruct, frozen=True, kw_only=True):
    """One structured agent compilation failure.

    Args:
        code:      Machine-readable :class:`AgentErrorCode`.
        message:   Human-readable description; the aggregated exception text is
                   built from these messages.
        component: Artifact, agent or config section the issue points at
                   (for example ``"agents/triage.agent.yaml"``).
        field:     Optional field path involved (for example
                   ``"capabilities[0].url"``).
    """

    code: AgentErrorCode
    message: str
    component: str = ""
    field: str | None = None


def from_message(message: str) -> AgentCompilationIssue:
    """Wrap a bare string as an :data:`AgentErrorCode.UNSPECIFIED` issue."""
    return AgentCompilationIssue(code=AgentErrorCode.UNSPECIFIED, message=message)


class AgentCompilationError(Exception):
    """Raised when one or more agent artifacts fail to compile.

    Aggregates every :class:`AgentCompilationIssue` found in a compilation run
    so a generator sees the whole picture instead of one failure at a time.

    Attributes:
        issues: Structured issues, one per failure.

    Args:
        issues: Issues (or bare message strings) collected by the compiler.
            Strings are normalised through :func:`from_message`.
    """

    def __init__(self, issues: Sequence[AgentCompilationIssue | str]) -> None:
        self.issues: tuple[AgentCompilationIssue, ...] = tuple(
            from_message(item) if isinstance(item, str) else item for item in issues
        )
        messages = [issue.message for issue in self.issues]
        super().__init__(
            f"Agent compilation failed with {len(messages)} error(s): {'; '.join(messages)}"
        )


# ---------------------------------------------------------------------------
# Envelope and structure factories
# ---------------------------------------------------------------------------


def spec_version_missing(component: str) -> AgentCompilationIssue:
    """An artifact declares no ``spec_version``, so it cannot be routed."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SPEC_VERSION_MISSING,
        message=f"{component}: spec_version is missing; it must be declared first",
        component=component,
        field="spec_version",
    )


def spec_version_unsupported(
    component: str,
    found: int,
    supported: Sequence[int],
) -> AgentCompilationIssue:
    """The declared spec version is not understood by this release."""
    known = ", ".join(str(version) for version in supported)
    return AgentCompilationIssue(
        code=AgentErrorCode.SPEC_VERSION_UNSUPPORTED,
        message=f"{component}: spec_version {found} is not supported; supported versions: {known}",
        component=component,
        field="spec_version",
    )


def spec_unknown_field(component: str, field: str) -> AgentCompilationIssue:
    """An unrecognised field appeared in the artifact; it is never ignored."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SPEC_UNKNOWN_FIELD,
        message=f"{component}: unknown field '{field}'; unknown fields are rejected",
        component=component,
        field=field,
    )


def spec_malformed(
    component: str,
    reason: str,
    field: str | None = None,
) -> AgentCompilationIssue:
    """The artifact is not decodable as the version it declares."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SPEC_MALFORMED,
        message=f"{component}: malformed artifact: {reason}",
        component=component,
        field=field,
    )


def agent_name_invalid(component: str, reason: str) -> AgentCompilationIssue:
    """The agent name does not satisfy the published name pattern."""
    return AgentCompilationIssue(
        code=AgentErrorCode.AGENT_NAME_INVALID,
        message=f"{component}: invalid agent name: {reason}",
        component=component,
        field="name",
    )


def agent_name_duplicate(name: str, sources: Sequence[str]) -> AgentCompilationIssue:
    """Two artifacts in the same application declare the same agent name.

    Args:
        name: The duplicated agent name.
        sources: Artifact paths that declare it, one entry per occurrence.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.AGENT_NAME_DUPLICATE,
        message=f"agent '{name}' is declared more than once: {', '.join(sources)}",
        component=name,
        field="name",
    )


# ---------------------------------------------------------------------------
# Output factories
# ---------------------------------------------------------------------------


def output_schema_invalid(component: str, reason: str) -> AgentCompilationIssue:
    """The declared output schema is not a valid JSON Schema object."""
    return AgentCompilationIssue(
        code=AgentErrorCode.OUTPUT_SCHEMA_INVALID,
        message=f"{component}: output schema is not a valid JSON Schema: {reason}",
        component=component,
        field="output.schema",
    )


def output_type_ref_unresolvable(component: str, ref: str) -> AgentCompilationIssue:
    """The ``module:Symbol`` output reference cannot be imported."""
    return AgentCompilationIssue(
        code=AgentErrorCode.OUTPUT_TYPE_REF_UNRESOLVABLE,
        message=f"{component}: output type reference '{ref}' cannot be imported",
        component=component,
        field="output.ref",
    )


def output_type_ref_unsupported(component: str, ref: str, reason: str) -> AgentCompilationIssue:
    """The output reference resolves to a type the engine cannot use."""
    return AgentCompilationIssue(
        code=AgentErrorCode.OUTPUT_TYPE_REF_UNSUPPORTED,
        message=f"{component}: output type reference '{ref}' is unsupported: {reason}",
        component=component,
        field="output.ref",
    )


def output_check_unresolvable(component: str, ref: str) -> AgentCompilationIssue:
    """The ``module:symbol`` output check reference cannot be imported."""
    return AgentCompilationIssue(
        code=AgentErrorCode.OUTPUT_CHECK_UNRESOLVABLE,
        message=f"{component}: output check '{ref}' cannot be imported",
        component=component,
        field="output_check",
    )


_ON_OUTPUT_USECASE_FIELD: Final[str] = "on_output.usecase"
"""Spec field every ``on_output`` compilation issue points at."""


def on_output_usecase_unknown(component: str, key: str) -> AgentCompilationIssue:
    """The output hook names a use-case key absent from the registry."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ON_OUTPUT_USECASE_UNKNOWN,
        message=f"{component}: on_output use case '{key}' is not registered",
        component=component,
        field=_ON_OUTPUT_USECASE_FIELD,
    )


def on_output_input_unsatisfied(component: str, key: str, reason: str) -> AgentCompilationIssue:
    """The hook cannot build the use case's Input from the run context and output."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ON_OUTPUT_INPUT_UNSATISFIED,
        message=(f"{component}: on_output use case '{key}' cannot be fed from the run: {reason}"),
        component=component,
        field=_ON_OUTPUT_USECASE_FIELD,
    )


def on_output_usecase_also_granted(component: str, key: str) -> AgentCompilationIssue:
    """The hook's use case is also granted to the model as a capability."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ON_OUTPUT_USECASE_ALSO_GRANTED,
        message=(
            f"{component}: on_output use case '{key}' is also granted as a capability; "
            "a hook use case must not be callable by the model"
        ),
        component=component,
        field=_ON_OUTPUT_USECASE_FIELD,
    )


INVOKER_MISSING_REASON: Final[str] = "no use-case invoker is configured"
"""Default ``reason`` of the invoker-missing issues: the deps bundle carries no invoker."""


def on_output_invoker_missing(
    agents: Sequence[str], *, reason: str = INVOKER_MISSING_REASON
) -> AgentCompilationIssue:
    """Agents declare an output hook but the deployment has no usable use-case invoker.

    Args:
        agents: Names of the agents declaring a hook.
        reason: What is wrong with the invoker, when it is not simply absent.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.ON_OUTPUT_INVOKER_MISSING,
        message=f"agents declare an output hook but {reason}: {', '.join(agents)}",
        component="ai",
        field="on_output",
    )


_CONVERSATION_USECASE_FIELD: Final[str] = "conversation.usecase"
"""Spec field every ``conversation`` compilation issue points at."""


def conversation_usecase_unknown(component: str, key: str) -> AgentCompilationIssue:
    """The conversation loader names a use-case key absent from the registry."""
    return AgentCompilationIssue(
        code=AgentErrorCode.CONVERSATION_USECASE_UNKNOWN,
        message=f"{component}: conversation use case '{key}' is not registered",
        component=component,
        field=_CONVERSATION_USECASE_FIELD,
    )


def conversation_input_unsatisfied(component: str, key: str, reason: str) -> AgentCompilationIssue:
    """The loader cannot build the use case's Input from the run context."""
    return AgentCompilationIssue(
        code=AgentErrorCode.CONVERSATION_INPUT_UNSATISFIED,
        message=(
            f"{component}: conversation use case '{key}' cannot be fed from the run: {reason}"
        ),
        component=component,
        field=_CONVERSATION_USECASE_FIELD,
    )


def conversation_usecase_also_granted(component: str, key: str) -> AgentCompilationIssue:
    """The loader's use case is also granted to the model as a capability."""
    return AgentCompilationIssue(
        code=AgentErrorCode.CONVERSATION_USECASE_ALSO_GRANTED,
        message=(
            f"{component}: conversation use case '{key}' is also granted as a capability; "
            "a loader use case must not be callable by the model"
        ),
        component=component,
        field=_CONVERSATION_USECASE_FIELD,
    )


def conversation_invoker_missing(
    agents: Sequence[str], *, reason: str = INVOKER_MISSING_REASON
) -> AgentCompilationIssue:
    """Agents declare a conversation loader but the deployment has no usable use-case invoker.

    Args:
        agents: Names of the agents declaring a loader.
        reason: What is wrong with the invoker, when it is not simply absent.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.CONVERSATION_INVOKER_MISSING,
        message=f"agents declare a conversation loader but {reason}: {', '.join(agents)}",
        component="ai",
        field="conversation",
    )


# ---------------------------------------------------------------------------
# State factories
# ---------------------------------------------------------------------------


def state_declaration_conflict(component: str) -> AgentCompilationIssue:
    """Both ``deps_type`` and ``deps_schema`` are declared; only one may be."""
    return AgentCompilationIssue(
        code=AgentErrorCode.STATE_DECLARATION_CONFLICT,
        message=(
            f"{component}: both 'deps_type' and 'deps_schema' are declared; declare at most one"
        ),
        component=component,
        field=_FIELD_DEPS_TYPE,
    )


def state_type_ref_unresolvable(component: str, ref: str) -> AgentCompilationIssue:
    """The ``module:Symbol`` state reference cannot be imported."""
    return AgentCompilationIssue(
        code=AgentErrorCode.STATE_TYPE_REF_UNRESOLVABLE,
        message=f"{component}: state type reference '{ref}' cannot be imported",
        component=component,
        field=_FIELD_DEPS_TYPE,
    )


def state_type_ref_unsupported(component: str, ref: str, reason: str) -> AgentCompilationIssue:
    """The state reference resolves to a symbol no schema can be derived from."""
    return AgentCompilationIssue(
        code=AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED,
        message=f"{component}: state type reference '{ref}' is unsupported: {reason}",
        component=component,
        field=_FIELD_DEPS_TYPE,
    )


def state_schema_invalid(component: str, reason: str) -> AgentCompilationIssue:
    """The declared state schema is not a valid JSON Schema object."""
    return AgentCompilationIssue(
        code=AgentErrorCode.STATE_SCHEMA_INVALID,
        message=f"{component}: state schema is not a valid JSON Schema: {reason}",
        component=component,
        field=_FIELD_DEPS_SCHEMA,
    )


def state_surface_unsupported(component: str, surface: str) -> AgentCompilationIssue:
    """A stateful artifact is exposed over a run surface that carries no state."""
    return AgentCompilationIssue(
        code=AgentErrorCode.STATE_SURFACE_UNSUPPORTED,
        message=(
            f"{component}: declares state and is exposed over '{surface}', which carries no state"
        ),
        component=component,
        field=_FIELD_DEPS_TYPE,
    )


# ---------------------------------------------------------------------------
# Instruction factories
# ---------------------------------------------------------------------------


def instruction_block_invalid(component: str, reason: str) -> AgentCompilationIssue:
    """An authored instruction block violates a compile-time rule."""
    return AgentCompilationIssue(
        code=AgentErrorCode.INSTRUCTION_BLOCK_INVALID,
        message=f"{component}: instruction block is invalid: {reason}",
        component=component,
        field=_FIELD_INSTRUCTIONS,
    )


def template_compilation_failed(component: str, block: str, reason: str) -> AgentCompilationIssue:
    """A templated instruction block fails to compile against its declared state.

    Args:
        component: Artifact the block belongs to.
        block: The block's name, or its position when it has none.
        reason: The template checker's own message, interpolated verbatim.
            A compilation error can carry a fragment of the source template,
            so redacting it — if the caller needs that — is the caller's
            responsibility, not this factory's.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.TEMPLATE_COMPILATION_FAILED,
        message=f"{component}: instruction block '{block}' fails to compile: {reason}",
        component=component,
        field=_FIELD_INSTRUCTIONS,
    )


def template_extra_missing(component: str, block: str, extra: str) -> AgentCompilationIssue:
    """A block declares ``template:`` while the templating extra is not installed.

    Args:
        component: Artifact the block belongs to.
        block: The block's name, or its position when it has none.
        extra: The optional dependency extra that installs the templating engine.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.TEMPLATE_EXTRA_MISSING,
        message=(
            f"{component}: instruction block '{block}' declares 'template: handlebars' "
            f"but the '{extra}' extra is not installed"
        ),
        component=component,
        field=_FIELD_INSTRUCTIONS,
    )


# ---------------------------------------------------------------------------
# Capability factories
# ---------------------------------------------------------------------------


def capability_kind_unsupported(component: str, kind: str, engine: str) -> AgentCompilationIssue:
    """The configured engine does not serve this capability kind."""
    return AgentCompilationIssue(
        code=AgentErrorCode.CAPABILITY_KIND_UNSUPPORTED,
        message=f"{component}: engine '{engine}' does not support capability kind '{kind}'",
        component=component,
        field="capabilities",
    )


def streaming_required_by_capability(component: str) -> AgentCompilationIssue:
    """A granted capability keeps streaming, so ``streaming: false`` cannot be honoured."""
    return AgentCompilationIssue(
        code=AgentErrorCode.STREAMING_REQUIRED_BY_CAPABILITY,
        message=(
            f"{component}: bound with 'streaming: false', but a granted capability requires a "
            "streamed run, so the binding cannot ask for whole answers"
        ),
        component=component,
        field="ai.models",
    )


def native_tool_unsupported(
    component: str,
    *,
    tool: str,
    role: str,
    provider: str,
    model: str,
    supported: Sequence[str],
) -> AgentCompilationIssue:
    """The model bound to this role cannot run the requested provider tool."""
    admitted = ", ".join(supported) or "none"
    return AgentCompilationIssue(
        code=AgentErrorCode.NATIVE_TOOL_UNSUPPORTED,
        message=(
            f"{component}: native tool '{tool}' is not supported by the model bound to role "
            f"'{role}' (provider '{provider}', model '{model}'); that binding supports: "
            f"{admitted}"
        ),
        component=component,
        field="capabilities.tool",
    )


def native_tool_duplicate(component: str, tool: str) -> AgentCompilationIssue:
    """The same provider tool is granted twice to one agent."""
    return AgentCompilationIssue(
        code=AgentErrorCode.NATIVE_TOOL_DUPLICATE,
        message=f"{component}: native tool '{tool}' is granted more than once",
        component=component,
        field="capabilities.tool",
    )


def capability_empty(component: str, kind: str) -> AgentCompilationIssue:
    """A capability entry grants nothing at all."""
    return AgentCompilationIssue(
        code=AgentErrorCode.CAPABILITY_EMPTY,
        message=f"{component}: capability '{kind}' grants no tool",
        component=component,
        field="capabilities",
    )


def usecase_key_unknown(component: str, key: str) -> AgentCompilationIssue:
    """A granted use-case key is absent from the use-case registry."""
    return AgentCompilationIssue(
        code=AgentErrorCode.USECASE_KEY_UNKNOWN,
        message=f"{component}: use-case key '{key}' is not registered",
        component=component,
        field="capabilities.keys",
    )


def sql_connection_unknown(component: str, connection: str) -> AgentCompilationIssue:
    """A ``sql`` capability names a connection that is not configured."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_CONNECTION_UNKNOWN,
        message=f"{component}: sql connection '{connection}' is not configured",
        component=component,
        field=_FIELD_CAPABILITIES_CONNECTION,
    )


def sql_connection_not_readonly(component: str, connection: str) -> AgentCompilationIssue:
    """A ``sql`` capability names a connection that permits writes."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_CONNECTION_NOT_READONLY,
        message=f"{component}: sql connection '{connection}' is not read-only",
        component=component,
        field=_FIELD_CAPABILITIES_CONNECTION,
    )


def sql_config_missing(component: str) -> AgentCompilationIssue:
    """A ``sql`` capability was declared with no data-layer config to validate."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_CONFIG_MISSING,
        message=f"{component}: sql capability declared with no data-layer configuration",
        component=component,
        field=_FIELD_CAPABILITIES_CONNECTION,
    )


def sql_connection_roles_unbound(component: str, connection: str) -> AgentCompilationIssue:
    """The connection's roles cannot be bound to a caller identity."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_CONNECTION_ROLES_UNBOUND,
        message=(
            f"{component}: roles of sql connection '{connection}' cannot be bound "
            f"to a caller identity"
        ),
        component=component,
        field=_FIELD_CAPABILITIES_CONNECTION,
    )


def sql_result_bound_missing(component: str, connection: str) -> AgentCompilationIssue:
    """A ``sql`` capability declares no result bounds."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_RESULT_BOUND_MISSING,
        message=(
            f"{component}: sql connection '{connection}' declares no max_rows / "
            f"max_result_bytes bound"
        ),
        component=component,
        field="capabilities.max_rows",
    )


def mcp_server_unknown(component: str, server: str) -> AgentCompilationIssue:
    """An ``mcp`` capability names a server that is not configured."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_SERVER_UNKNOWN,
        message=f"{component}: mcp server '{server}' is not configured in ai.mcp_servers",
        component=component,
        field="capabilities.server",
    )


def mcp_url_invalid(component: str, url: str, reason: str) -> AgentCompilationIssue:
    """An MCP server URL is malformed, not ``https://``, or carries credentials."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_URL_INVALID,
        message=f"{component}: invalid mcp url '{url}': {reason}",
        component=component,
        field="url",
    )


def mcp_credentials_inline(component: str, field: str) -> AgentCompilationIssue:
    """Credentials were written into the artifact instead of being referenced."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_CREDENTIALS_INLINE,
        message=(
            f"{component}: '{field}' carries inline credentials; reference them "
            f"through deployment configuration instead"
        ),
        component=component,
        field=field,
    )


def mcp_headers_ref_invalid(component: str) -> AgentCompilationIssue:
    """A resolved ``headers_ref`` payload is not one ``Name=value`` header pair."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_HEADERS_REF_INVALID,
        message=(
            f"{component}: 'headers_ref' must resolve to one 'Name=value' header "
            f"pair; use an 'auth' strategy for anything richer"
        ),
        component=component,
        field="headers_ref",
    )


def mcp_auth_conflict(component: str) -> AgentCompilationIssue:
    """A server sets both ``headers_ref`` and ``auth``, two credentials for one connection."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_AUTH_CONFLICT,
        message=(
            f"{component}: 'headers_ref' and 'auth' are mutually exclusive; "
            f"one connection carries one credential"
        ),
        component=component,
        field="auth",
    )


def mcp_transport_invalid(
    component: str, reason: str, *, field: str = "transport"
) -> AgentCompilationIssue:
    """An MCP server declares fields that its ``transport`` does not accept, or an unknown one.

    Args:
        component: Configuration entry the issue points at.
        reason: What about the transport is invalid.
        field: Configuration key to blame; the transport itself unless a nested
            key, such as one ``env`` name, is the culprit.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_TRANSPORT_INVALID,
        message=f"{component}: invalid mcp transport: {reason}",
        component=component,
        field=field,
    )


def mcp_auth_strategy_unknown(
    component: str, kind: str, available: Sequence[str]
) -> AgentCompilationIssue:
    """A named auth strategy resolves to no entry point in ``loom.ai.remote_auth``."""
    installed = ", ".join(available) if available else "none"
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_AUTH_STRATEGY_UNKNOWN,
        message=(
            f"{component}: auth strategy '{kind}' is not registered in entry-point "
            f"group 'loom.ai.remote_auth'; registered: {installed}"
        ),
        component=component,
        field="auth.kind",
    )


def mcp_auth_strategy_invalid(kind: str, reason: str) -> AgentCompilationIssue:
    """A registered auth strategy could not be constructed, or is unusable."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_AUTH_STRATEGY_INVALID,
        message=f"auth strategy '{kind}' is unusable: {reason}",
        component=f"loom.ai.remote_auth:{kind}",
        field="auth.kind",
    )


def skills_library_invalid(component: str, library: str, reason: str) -> AgentCompilationIssue:
    """A skill library does not resolve to a readable library directory."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SKILLS_LIBRARY_INVALID,
        message=f"{component}: skills library '{library}' is unusable: {reason}",
        component=component,
        field=_FIELD_CAPABILITIES_LIBRARY,
    )


def skills_library_escapes(component: str, library: str) -> AgentCompilationIssue:
    """A skill library resolves outside the directory it is anchored to."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SKILLS_LIBRARY_ESCAPES,
        message=f"{component}: skills library '{library}' escapes its own directory",
        component=component,
        field=_FIELD_CAPABILITIES_LIBRARY,
    )


def skills_name_collision(
    component: str,
    skill: str,
    first_library: str,
    second_library: str,
) -> AgentCompilationIssue:
    """Two libraries granted to one agent expose the same skill name."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SKILLS_NAME_COLLISION,
        message=(
            f"{component}: skill '{skill}' is exposed by both libraries "
            f"'{first_library}' and '{second_library}'"
        ),
        component=component,
        field=_FIELD_CAPABILITIES_LIBRARY,
    )


def skills_root_missing(component: str) -> AgentCompilationIssue:
    """A bare skill library was named with no ``skills_root`` configured."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SKILLS_ROOT_MISSING,
        message=(
            f"{component}: a bare skills library requires a configured skills_root; "
            f"use './name' to resolve it beside the artifact instead"
        ),
        component=component,
        field=_FIELD_CAPABILITIES_LIBRARY,
    )


def python_factory_unresolvable(component: str, factory: str, reason: str) -> AgentCompilationIssue:
    """A ``python`` capability factory cannot be imported."""
    return AgentCompilationIssue(
        code=AgentErrorCode.PYTHON_FACTORY_UNRESOLVABLE,
        message=f"{component}: python factory '{factory}' cannot be imported: {reason}",
        component=component,
        field=_FIELD_CAPABILITIES_FACTORY,
    )


def python_factory_not_callable(component: str, factory: str) -> AgentCompilationIssue:
    """A ``python`` capability factory does not satisfy ``ToolsetFactory``."""
    return AgentCompilationIssue(
        code=AgentErrorCode.PYTHON_FACTORY_NOT_CALLABLE,
        message=f"{component}: python factory '{factory}' does not satisfy ToolsetFactory",
        component=component,
        field=_FIELD_CAPABILITIES_FACTORY,
    )


def python_factory_params_rejected(
    component: str, factory: str, reason: str
) -> AgentCompilationIssue:
    """A ``python`` capability's ``params`` do not bind to the factory's signature."""
    return AgentCompilationIssue(
        code=AgentErrorCode.PYTHON_FACTORY_PARAMS_REJECTED,
        message=f"{component}: python factory '{factory}' rejects params: {reason}",
        component=component,
        field="capabilities.params",
    )


def python_remote_not_granted(component: str, factory: str, server: str) -> AgentCompilationIssue:
    """A ``python`` factory asked for an MCP server its agent was not granted."""
    return AgentCompilationIssue(
        code=AgentErrorCode.PYTHON_REMOTE_NOT_GRANTED,
        message=(
            f"{component}: python factory '{factory}' asked for mcp server '{server}', "
            f"but agent '{component}' has no mcp grant on that server"
        ),
        component=component,
        field=_FIELD_CAPABILITIES_FACTORY,
    )


def python_factory_failed(component: str, factory: str, error: str) -> AgentCompilationIssue:
    """A ``python`` factory raised while building its toolset at start-up.

    Only the exception class is named: the message could carry a ``params``
    value or anything else the factory touched.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.PYTHON_FACTORY_FAILED,
        message=f"{component}: python factory '{factory}' raised {error} while building",
        component=component,
        field=_FIELD_CAPABILITIES_FACTORY,
    )


def a2a_agent_unknown(component: str, agent: str) -> AgentCompilationIssue:
    """An ``a2a`` capability names a remote agent that is not configured."""
    return AgentCompilationIssue(
        code=AgentErrorCode.A2A_AGENT_UNKNOWN,
        message=f"{component}: a2a agent '{agent}' is not configured in ai.a2a_agents",
        component=component,
        field="capabilities.agent",
    )


def a2a_url_invalid(component: str, url: str, reason: str) -> AgentCompilationIssue:
    """A remote agent URL is malformed, not ``https://``, or carries credentials."""
    return AgentCompilationIssue(
        code=AgentErrorCode.A2A_URL_INVALID,
        message=f"{component}: invalid a2a url '{url}': {reason}",
        component=component,
        field="url",
    )


def anonymous_with_data_capability(component: str, kind: str) -> AgentCompilationIssue:
    """An unauthenticated agent holds a data or remote capability."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ANONYMOUS_WITH_DATA_CAPABILITY,
        message=(
            f"{component}: agent opts out of authentication while holding the '{kind}' capability"
        ),
        component=component,
        field="capabilities",
    )


# ---------------------------------------------------------------------------
# Model and policy factories
# ---------------------------------------------------------------------------


def model_role_unbound(component: str, role: str) -> AgentCompilationIssue:
    """The agent's model role is not present in ``ai.models``."""
    return AgentCompilationIssue(
        code=AgentErrorCode.MODEL_ROLE_UNBOUND,
        message=f"{component}: model role '{role}' is not bound in ai.models",
        component=component,
        field="model_role",
    )


def inference_target_incomplete(role: str, setting: str) -> AgentCompilationIssue:
    """A model-role binding lacks a setting its provider requires."""
    return AgentCompilationIssue(
        code=AgentErrorCode.INFERENCE_TARGET_INCOMPLETE,
        message=f"model role '{role}': required setting '{setting}' is missing",
        component=f"model role '{role}'",
        field=setting,
    )


def output_mode_unknown(role: str, value: str, valid: Sequence[str]) -> AgentCompilationIssue:
    """A model-role binding names an ``output_mode`` loom does not offer.

    The valid set is a parameter, as in every sibling factory that names one:
    it keeps this module free of domain imports.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.OUTPUT_MODE_UNKNOWN,
        message=(f"model role '{role}': output_mode '{value}' is not one of {', '.join(valid)}"),
        component=f"model role '{role}'",
        field="output_mode",
    )


def output_mode_unsupported(
    provider: str, mode: str, supported: Sequence[str]
) -> AgentCompilationIssue:
    """A binding pins an ``output_mode`` the bound provider cannot serve.

    Distinct from ``OUTPUT_MODE_UNKNOWN``: the mode is one loom offers, but
    this provider's models refuse it on every request, so the pin is known
    to be wrong before the first one.

    Args:
        provider: Provider identifier the binding named.
        mode: The pinned mode.
        supported: Modes the provider does serve.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.OUTPUT_MODE_UNSUPPORTED,
        message=(
            f"provider '{provider}': output_mode '{mode}' is not served by its models; "
            f"supported modes: {', '.join(supported)}"
        ),
        component=provider,
        field="output_mode",
    )


def policy_out_of_range(
    component: str,
    policy: str,
    value: int | Decimal,
    minimum: int | Decimal,
    maximum: int | Decimal,
) -> AgentCompilationIssue:
    """A policy value falls outside its documented range.

    ``value``, ``minimum`` and ``maximum`` accept ``Decimal`` as well as
    ``int``: every policy value is an integer count except ``max_usd``, which
    is a ``Decimal`` (FR-043), and the two share this one factory rather than
    each carrying its own near-identical message.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.POLICY_OUT_OF_RANGE,
        message=(
            f"{component}: policy '{policy}' value {value} is outside the "
            f"allowed range {minimum}..{maximum}"
        ),
        component=component,
        field=f"policies.{policy}",
    )


# ---------------------------------------------------------------------------
# Deployment resolution factories
# ---------------------------------------------------------------------------


def engine_not_found(name: str, available: Sequence[str]) -> AgentCompilationIssue:
    """No installed entry point provides the requested engine."""
    known = ", ".join(available) if available else "none"
    return AgentCompilationIssue(
        code=AgentErrorCode.ENGINE_NOT_FOUND,
        message=f"engine '{name}' is not installed; available engines: {known}",
        component=name,
        field=_FIELD_AI_ENGINE,
    )


def engine_duplicate(name: str, distributions: Sequence[str]) -> AgentCompilationIssue:
    """Two distributions claim the same engine entry-point name."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ENGINE_DUPLICATE,
        message=(
            f"engine '{name}' is provided by more than one distribution: {', '.join(distributions)}"
        ),
        component=name,
        field=_FIELD_AI_ENGINE,
    )


def engine_api_mismatch(name: str, found: int, supported: Sequence[int]) -> AgentCompilationIssue:
    """An engine announces a handshake version this release cannot speak."""
    known = ", ".join(str(version) for version in supported)
    return AgentCompilationIssue(
        code=AgentErrorCode.ENGINE_API_MISMATCH,
        message=(f"engine '{name}' speaks handshake version {found}; supported versions: {known}"),
        component=name,
        field=_FIELD_AI_ENGINE,
    )


def provider_not_installed(provider: str, extra: str) -> AgentCompilationIssue:
    """A provider SDK is missing; the message names the extra to install."""
    return AgentCompilationIssue(
        code=AgentErrorCode.PROVIDER_NOT_INSTALLED,
        message=f"provider '{provider}' is not installed; install the '{extra}' extra",
        component=provider,
    )


def provider_unknown(provider: str, supported: Sequence[str]) -> AgentCompilationIssue:
    """The provider is not one this release knows how to bind.

    Distinct from ``PROVIDER_NOT_INSTALLED``: there is no extra to install,
    because no such provider exists in this release.

    Args:
        provider: Provider identifier the artifact named.
        supported: Provider identifiers this release binds.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.PROVIDER_UNKNOWN,
        message=(
            f"provider '{provider}' is not known to this release of loom; "
            f"supported providers: {', '.join(supported)}"
        ),
        component=provider,
    )


def provider_setting_missing(provider: str, setting: str) -> AgentCompilationIssue:
    """A provider setting (credentials, region, endpoint) is absent."""
    return AgentCompilationIssue(
        code=AgentErrorCode.PROVIDER_SETTING_MISSING,
        message=f"provider '{provider}': required setting '{setting}' is missing",
        component=provider,
        field=setting,
    )


def mcp_connection_conflict(server: str, agents: Sequence[str]) -> AgentCompilationIssue:
    """One MCP server name resolves to two different connections in one worker.

    Args:
        server: The registered server name, never its URL — a URL carries
            credentials and hosts the redaction guarantee keeps out of
            diagnostics (FR-030a/FR-038).
        agents: Names of the two agents whose grants disagree, in plan order.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_CONNECTION_CONFLICT,
        message=(
            f"mcp server '{server}' resolves to different connections for agents "
            f"{', '.join(agents)}: one worker opens a single client per server, so "
            f"the transport, address, credential and deadline of every grant of that "
            f"name must be identical"
        ),
        component=server,
        field="capabilities.server",
    )


def mcp_server_unreachable(server: str, reason: str) -> AgentCompilationIssue:
    """An MCP server is not reachable at start-up.

    Args:
        server: The server's registered name, never its URL — a URL carries
            credentials and hosts that the redaction guarantee keeps out of
            diagnostics (FR-030a/FR-038).
        reason: Why the connection did not complete.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_SERVER_UNREACHABLE,
        message=f"mcp server '{server}' is unreachable: {reason}",
        component=server,
    )


def tool_filter_matches_nothing(component: str, target: str) -> AgentCompilationIssue:
    """An include/exclude filter excludes every tool the target exposes."""
    return AgentCompilationIssue(
        code=AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING,
        message=f"{component}: filter for '{target}' matches no tool",
        component=component,
        field="capabilities.include",
    )


def sql_readonly_drift(connection: str) -> AgentCompilationIssue:
    """Live configuration contradicts the plan's read-only assumption."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SQL_READONLY_DRIFT,
        message=(
            f"sql connection '{connection}' is no longer read-only; the compiled "
            f"plan assumed it was"
        ),
        component=connection,
    )


def endpoint_auth_missing(component: str) -> AgentCompilationIssue:
    """An agent opted into HTTP exposure without naming its authentication."""
    return AgentCompilationIssue(
        code=AgentErrorCode.ENDPOINT_AUTH_MISSING,
        message=f"{component}: HTTP exposure requires a named authentication",
        component=component,
        field="auth",
    )


def a2a_base_url_invalid(url: str, reason: str) -> AgentCompilationIssue:
    """Report an ``ai.a2a.base_url`` that is unsafe to publish.

    Args:
        url: The offending URL, already redacted of userinfo and query.
        reason: Why it is unsafe, in the vocabulary of the URL check.

    Returns:
        The issue, coded :data:`AgentErrorCode.A2A_BASE_URL_INVALID`.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.A2A_BASE_URL_INVALID,
        component="ai.a2a",
        field="base_url",
        message=f"the published card base URL is unsafe: {reason} ({url})",
    )


def a2a_expose_empty() -> AgentCompilationIssue:
    """A2A exposure was enabled without naming a single agent."""
    return AgentCompilationIssue(
        code=AgentErrorCode.A2A_EXPOSE_EMPTY,
        message="a2a exposure is enabled but names no agent",
        component="a2a",
        field="a2a.expose",
    )


def auth_exclusion_overlaps_agents(paths: Sequence[str]) -> AgentCompilationIssue:
    """An authentication exclusion covers an agent or A2A invocation path."""
    return AgentCompilationIssue(
        code=AgentErrorCode.AUTH_EXCLUSION_OVERLAPS_AGENTS,
        message=(f"authentication exclusions cover agent invocation paths: {', '.join(paths)}"),
        component="auth",
        field="auth.exclude",
    )


def a2a_agent_unreachable(agent: str, reason: str) -> AgentCompilationIssue:
    """A remote agent's card cannot be retrieved.

    Args:
        agent: The remote agent's registered name, never its URL — a URL
            carries credentials and hosts that the redaction guarantee keeps
            out of diagnostics (FR-030a/FR-038).
        reason: Why the card could not be retrieved.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.A2A_AGENT_UNREACHABLE,
        message=f"remote a2a agent '{agent}' is unreachable: {reason}",
        component=agent,
    )


def agent_specs_conflict() -> AgentCompilationIssue:
    """Both artifact sources declare agents; there is no implicit precedence."""
    return AgentCompilationIssue(
        code=AgentErrorCode.AGENT_SPECS_CONFLICT,
        message=(
            "agent artifacts are declared both by the manifest 'AGENTS' attribute and by "
            "the 'ai.specs' config key; declare them in exactly one of the two"
        ),
        component="ai",
        field="ai.specs",
    )


def agent_specs_missing() -> AgentCompilationIssue:
    """The ``ai:`` section is configured but no artifact source declares agents."""
    return AgentCompilationIssue(
        code=AgentErrorCode.AGENT_SPECS_MISSING,
        message=(
            "the 'ai:' section is configured but declares no agent artifact; set 'ai.specs' "
            "or the manifest 'AGENTS' attribute"
        ),
        component="ai",
        field="ai.specs",
    )


def agent_specs_escape_root(pattern: str) -> AgentCompilationIssue:
    """An artifact glob reaches outside the root it is resolved against."""
    return AgentCompilationIssue(
        code=AgentErrorCode.AGENT_SPECS_ESCAPE_ROOT,
        message=(
            f"agent artifact pattern '{pattern}' reaches outside the directory it is "
            "resolved against; write it relative to that directory, without '..'"
        ),
        component="ai",
        field="ai.specs",
    )


def remote_clients_unknown(value: str, valid: Sequence[str]) -> AgentCompilationIssue:
    """The start-up tolerance of remote clients names no known mode.

    Args:
        value: The rejected value of ``ai.remote_clients``.
        valid: The accepted modes, supplied by the caller so this module keeps
            no knowledge of the configuration domain.
    """
    accepted = ", ".join(f"'{mode}'" for mode in valid)
    return AgentCompilationIssue(
        code=AgentErrorCode.REMOTE_CLIENTS_UNKNOWN,
        message=(
            f"ai.remote_clients: '{value}' is not a known start-up mode for remote "
            f"clients; accepted: {accepted}"
        ),
        component="ai",
        field="ai.remote_clients",
    )


def max_agent_depth_invalid(value: int) -> AgentCompilationIssue:
    """The nesting bound is below the one entry every top-level run already spends.

    Args:
        value: The rejected value of ``ai.max_agent_depth``.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.MAX_AGENT_DEPTH_INVALID,
        message=(
            f"ai.max_agent_depth: {value} is below the minimum of 1; the top-level "
            "run itself counts as one entry in the chain, so a value below 1 refuses "
            "every run, including the top-level one"
        ),
        component="ai",
        field="ai.max_agent_depth",
    )


# ---------------------------------------------------------------------------
# Compatibility factories
# ---------------------------------------------------------------------------


def spec_version_deprecated(component: str, found: int, latest: int) -> AgentCompilationIssue:
    """The artifact's version is still accepted but has been superseded."""
    return AgentCompilationIssue(
        code=AgentErrorCode.SPEC_VERSION_DEPRECATED,
        message=(
            f"{component}: spec_version {found} is deprecated; version {latest} is the current one"
        ),
        component=component,
        field="spec_version",
    )


# ---------------------------------------------------------------------------
# Use-case marker factories (Agent() and Mcp())
# ---------------------------------------------------------------------------

_AGENT_MARKER_FIELD_TEMPLATE: Final[str] = "parameters.{parameter}"
"""Field-path template shared by all four use-case marker issues below:
two for ``Agent()`` (:func:`agent_marker_unknown`,
:func:`agent_marker_output_mismatch`) and two for ``Mcp()``
(:func:`mcp_marker_unknown`, :func:`use_case_tool_filter_matches_nothing`)."""


def agent_marker_unknown(
    usecase: str, parameter: str, agent: str, available: Sequence[str]
) -> AgentCompilationIssue:
    """A use case's :func:`~loom.core.use_case.markers.Agent` marker names an
    agent no engine compiled.

    Args:
        usecase: Registered key of the use case declaring the marker.
        parameter: Name of the ``execute`` parameter carrying the marker.
        agent: Agent name the marker declared.
        available: Names of the agents actually compiled in this deployment.
    """
    known = ", ".join(available) if available else "none"
    return AgentCompilationIssue(
        code=AgentErrorCode.AGENT_MARKER_UNKNOWN,
        message=(
            f"{usecase}: parameter '{parameter}' names unknown agent '{agent}'; "
            f"compiled agents: {known}"
        ),
        component=usecase,
        field=_AGENT_MARKER_FIELD_TEMPLATE.format(parameter=parameter),
    )


def mcp_marker_unknown(
    usecase: str, parameter: str, server: str, available: Sequence[str]
) -> AgentCompilationIssue:
    """A use case's :func:`~loom.core.use_case.markers.Mcp` marker names a
    server no engine compiled.

    The existing :func:`mcp_server_unknown` cannot serve this condition: it
    carries neither the parameter nor the available server names, both of
    which this message needs to point someone at the right signature.

    Args:
        usecase: Registered key of the use case declaring the marker.
        parameter: Name of the ``execute`` parameter carrying the marker.
        server: Server name the marker declared.
        available: Names of the servers actually configured for this
            deployment.
    """
    known = ", ".join(available) if available else "none"
    return AgentCompilationIssue(
        code=AgentErrorCode.MCP_MARKER_UNKNOWN,
        message=(
            f"{usecase}: parameter '{parameter}' names unknown mcp server '{server}'; "
            f"configured servers: {known}"
        ),
        component=usecase,
        field=_AGENT_MARKER_FIELD_TEMPLATE.format(parameter=parameter),
    )


def use_case_tool_filter_matches_nothing(
    usecase: str, parameter: str, server: str
) -> AgentCompilationIssue:
    """A use case's :func:`~loom.core.use_case.markers.Mcp` ``include`` matches
    no tool the named server publishes.

    A standalone factory, not a variant selected inside ``filter_issues``:
    the use-case check runs over the marker's own parameter, something
    ``filter_issues`` never carries a name for. It reuses
    :attr:`AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING` rather than minting a
    new code — one condition, one code — and differs from
    :func:`tool_filter_matches_nothing` in both message and ``field``: this
    one carries ``parameters.{parameter}`` (via
    :data:`_AGENT_MARKER_FIELD_TEMPLATE`) instead of
    ``capabilities.include``. That ``field`` difference is what makes reusing
    the shared code safe: it is the only thing that lets a caller filtering
    on :attr:`AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING` tell an
    agent-artifact issue apart from a use-case-parameter one.

    Args:
        usecase: Registered key of the use case declaring the marker.
        parameter: Name of the ``execute`` parameter carrying the marker.
        server: Server name the marker declared.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING,
        message=(
            f"{usecase}: parameter '{parameter}' declares an include filter for mcp "
            f"server '{server}' that matches no tool"
        ),
        component=usecase,
        field=_AGENT_MARKER_FIELD_TEMPLATE.format(parameter=parameter),
    )


def agent_marker_output_mismatch(
    usecase: str, parameter: str, agent: str, expected: str, declared: str
) -> AgentCompilationIssue:
    """A use case's ``AgentHandle`` annotation disagrees with the named
    agent's own declared output type.

    Args:
        usecase: Registered key of the use case declaring the marker.
        parameter: Name of the ``execute`` parameter carrying the marker.
        agent: Agent name the marker declared.
        expected: Output type named by the parameter's ``AgentHandle[...]``
            annotation.
        declared: Output type the named agent actually declares.
    """
    return AgentCompilationIssue(
        code=AgentErrorCode.AGENT_MARKER_OUTPUT_MISMATCH,
        message=(
            f"{usecase}: parameter '{parameter}' declares AgentHandle[{expected}] for "
            f"agent '{agent}', but '{agent}' declares output '{declared}'"
        ),
        component=usecase,
        field=_AGENT_MARKER_FIELD_TEMPLATE.format(parameter=parameter),
    )


# ---------------------------------------------------------------------------
# Run-time catalogue
# ---------------------------------------------------------------------------


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

    # Agent-handle grants and calls (model-as-actor)
    MCP_GRANT_UNKNOWN = "MCP_GRANT_UNKNOWN"
    SQL_GRANT_UNKNOWN = "SQL_GRANT_UNKNOWN"
    TOOL_UNKNOWN = "TOOL_UNKNOWN"
    TOOL_UNTYPED = "TOOL_UNTYPED"
    TOOL_RESULT_UNSTRUCTURED = "TOOL_RESULT_UNSTRUCTURED"
    TOOL_DECODE_FAILED = "TOOL_DECODE_FAILED"
    TOOL_CALL_FAILED = "TOOL_CALL_FAILED"
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
        # The cap was never evaluated here, unlike USAGE_LIMIT_EXCEEDED: a
        # gap in the price catalogue, not a run that spent too much. Classed
        # INFRASTRUCTURE — the artifact is not at fault — but carved out of
        # 'is_retriable' below; see that function and _NEVER_RETRIED.
        AgentRunErrorCode.COST_NOT_MEASURABLE: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.RUN_TIMEOUT: AgentRunErrorClass.LIMIT,
        AgentRunErrorCode.TOO_MANY_RUNS: AgentRunErrorClass.LIMIT,
        AgentRunErrorCode.UNAUTHORIZED: AgentRunErrorClass.AUTHORIZATION,
        AgentRunErrorCode.CANCELLED: AgentRunErrorClass.CLIENT,
        AgentRunErrorCode.HOOK_FAILED: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.CONVERSATION_LOAD_FAILED: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.CONVERSATION_LOAD_TIMEOUT: AgentRunErrorClass.INFRASTRUCTURE,
        # A caller-supplied 'state' against an artefact declaring none is a
        # calling-code mistake, not a transient condition (FR-010).
        AgentRunErrorCode.STATE_UNDECLARED: AgentRunErrorClass.APPLICATION,
        # An omitted 'state' against a shape with a field that has no
        # declared default cannot be filled in with defaults that do not
        # exist; this is a calling-code mistake, not a transient condition.
        AgentRunErrorCode.STATE_REQUIRED: AgentRunErrorClass.APPLICATION,
        # An unknown grant or tool name is a caller-code mistake, not a
        # transient condition; a per-run type mismatch is not the model
        # misbehaving, it is the calling code's own bug (AUTHORIZATION and
        # APPLICATION never retry). A server that contradicts its own
        # published schema, or a tool call that fails outright, sits beside
        # the existing TOOL_* infrastructure codes rather than inventing a
        # new class for one failure family.
        AgentRunErrorCode.MCP_GRANT_UNKNOWN: AgentRunErrorClass.AUTHORIZATION,
        AgentRunErrorCode.SQL_GRANT_UNKNOWN: AgentRunErrorClass.AUTHORIZATION,
        AgentRunErrorCode.TOOL_UNKNOWN: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.TOOL_UNTYPED: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.TOOL_DECODE_FAILED: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.TOOL_CALL_FAILED: AgentRunErrorClass.INFRASTRUCTURE,
        AgentRunErrorCode.AGENT_CALL_CYCLE: AgentRunErrorClass.APPLICATION,
        AgentRunErrorCode.AGENT_CALL_TOO_DEEP: AgentRunErrorClass.LIMIT,
        AgentRunErrorCode.AGENT_RUN_SHAPE_WITH_HOOK: AgentRunErrorClass.APPLICATION,
    }
)

CONVERSATION_LOAD_FAILED_MESSAGE: Final[str] = (
    "the conversation could not be loaded; the detail is recorded server-side"
)
"""Client text of every ``CONVERSATION_LOAD_FAILED`` error (D8).

Defined once here because both the runtime loader and an engine's history
decoder raise the code; the loader's own detail is logged, never returned.
"""

CONVERSATION_LOAD_TIMEOUT_MESSAGE: Final[str] = "the conversation loader exceeded its time limit"
"""Client text of every ``CONVERSATION_LOAD_TIMEOUT`` error (FR-063)."""


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


_NEVER_RETRIED: frozenset[AgentRunErrorCode] = frozenset({AgentRunErrorCode.COST_NOT_MEASURABLE})
"""``INFRASTRUCTURE``-classed codes that retrying can never help.

An explicit exception list, not a change to :data:`AgentRunErrorClass`:
``COST_NOT_MEASURABLE`` stays ``INFRASTRUCTURE`` (mapped to HTTP 500, not
503, so callers and gateways do not retry it without consulting
:func:`is_retriable`) but is raised only after the run's provider call has
already returned and already been billed, so retrying would only spend the
caller's money again on a gap it does not control."""


def is_retriable(code: AgentRunErrorCode) -> bool:
    """Return whether a run-time failure may be retried by the caller.

    Only :data:`AgentRunErrorClass.INFRASTRUCTURE` failures are retriable,
    with one explicit exception: ``COST_NOT_MEASURABLE`` stays
    ``INFRASTRUCTURE`` but is never retriable, because the provider call it
    reports on has already returned and already been billed.

    Args:
        code: Run-time error code to test.

    Returns:
        ``True`` when the code's class is ``INFRASTRUCTURE``; ``False`` for
        every other class, and for ``COST_NOT_MEASURABLE`` despite its
        ``INFRASTRUCTURE`` class.
    """
    if code in _NEVER_RETRIED:
        return False
    return run_error_class(code) is AgentRunErrorClass.INFRASTRUCTURE
