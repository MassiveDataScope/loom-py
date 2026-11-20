"""Tier-1 authored artifact structs for agent spec version 1.

These structs are the *authored* surface: what a human or a generator writes in
an ``.agent.yaml`` file. They are engine-agnostic and vendor-agnostic — no
engine, provider, model identifier or credential is representable here.

Every struct is frozen, keyword-only and rejects unknown fields, so an
unrecognised key is a decoding failure rather than a silently dropped value.

The module-level constants are the single source of truth for the published
JSON Schema: :mod:`loom.ai.declarative._schema` derives every pattern, default,
minimum and maximum from them so the schema cannot drift from the structs.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Annotated, Any, Final, Literal, get_args

import msgspec

SPEC_VERSION_V1: Final[int] = 1
"""Format version implemented by :class:`AgentSpecV1`."""

AGENT_NAME_PATTERN: Final[str] = r"^[a-z][a-z0-9_-]{0,62}$"
"""Pattern every agent name must satisfy."""

MODEL_ROLE_PATTERN: Final[str] = r"^[a-z][a-z0-9_-]{0,31}$"
"""Pattern every logical model role must satisfy."""

_SYMBOL_REF_BODY: Final[str] = r"[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*"
"""Unanchored body of a ``module:symbol`` reference, shared by every pattern
that embeds it as an alternative so anchoring it once cannot drift out of
sync with a sliced copy."""

SYMBOL_REF_PATTERN: Final[str] = rf"^{_SYMBOL_REF_BODY}$"
"""Pattern of a ``module:symbol`` reference; filesystem paths are not representable."""

DEPS_TYPE_PATTERN: Final[str] = rf"^(dict|{_SYMBOL_REF_BODY})$"
"""Pattern ``deps_type`` must satisfy: the literal ``dict`` or a ``module:Symbol``
reference matching :data:`SYMBOL_REF_PATTERN`. ``dict`` contains no colon, so
the two alternatives cannot collide."""

RESERVED_INSTRUCTION_NAME: Final[str] = "agent"
"""Instruction block name the engine reserves for itself
(``pydantic_ai._instructions.validate_instruction_name``)."""

_INSTRUCTION_NAME_BODY: Final[str] = r"[^:]+"

INSTRUCTION_NAME_PATTERN: Final[str] = rf"^{_INSTRUCTION_NAME_BODY}$"
"""Pattern an instruction block's ``name`` must satisfy: non-empty and no
``:``. This is the pattern the published JSON Schema emits; rejecting
:data:`RESERVED_INSTRUCTION_NAME` is expressed there through ``not``/``const``
composition rather than folded into this pattern, because a look-around
alternative compiles under Python's ``re`` but fails to compile under the
RE2-family validators (Go, some editor plugins) that consume the published
schema, which would make the whole document unusable rather than just this
constraint."""

_INSTRUCTION_NAME_DECODE_PATTERN: Final[str] = (
    rf"^(?!{RESERVED_INSTRUCTION_NAME}$){_INSTRUCTION_NAME_BODY}$"
)
"""Decode-time pattern: :data:`INSTRUCTION_NAME_PATTERN` plus rejecting
:data:`RESERVED_INSTRUCTION_NAME` via look-ahead. msgspec compiles this with
Python's ``re``, which supports look-around, so a reserved name fails by its
own name at decode time instead of surfacing later as an ``Agent``
construction error."""

SKILLS_LIBRARY_PATTERN: Final[str] = r"^(\./[A-Za-z0-9._-]+|[A-Za-z0-9._-]+)$"
"""Pattern of a skill library name: ``./name`` beside the artifact, or a bare name.

``..`` is not representable, so a library can never escape its own directory.
"""

DEFAULT_MODEL_ROLE: Final[str] = "default"
"""Model role an artifact binds to when it declares none."""

RETRIES_DEFAULT: Final[int] = 2
RETRIES_MIN: Final[int] = 0
RETRIES_MAX: Final[int] = 10

RETRIES_DESCRIPTION: Final[str] = (
    "Retries a failed tool call, and an answer output_check rejects, inside "
    "one run, always. Retries a failed provider call across runs, only when "
    "the plan holds no capability; see 'retries' in docs/ai/artifacts.md."
)
"""Single source of the ``retries`` field's published description; also cited
by :class:`PolicySpec`'s own docstring so the two never retype the same
sentence."""

TOOL_TIMEOUT_MS_DEFAULT: Final[int] = 20000
TOOL_TIMEOUT_MS_MIN: Final[int] = 100
TOOL_TIMEOUT_MS_MAX: Final[int] = 600000

MAX_ITERATIONS_DEFAULT: Final[int] = 12
MAX_ITERATIONS_MIN: Final[int] = 1
MAX_ITERATIONS_MAX: Final[int] = 100

RUN_TIMEOUT_MS_DEFAULT: Final[int] = 120000
RUN_TIMEOUT_MS_MIN: Final[int] = 1000
RUN_TIMEOUT_MS_MAX: Final[int] = 1800000

MAX_HISTORY_BYTES_DEFAULT: Final[int] = 1_048_576
MAX_HISTORY_BYTES_MIN: Final[int] = 1_024
MAX_HISTORY_BYTES_MAX: Final[int] = 67_108_864

MAX_USD_MIN: Final[Decimal] = Decimal("0.01")
MAX_USD_MAX: Final[Decimal] = Decimal("100000")

MAX_TOTAL_TOKENS_MIN: Final[int] = 1
MAX_TOTAL_TOKENS_MAX: Final[int] = 50_000_000

MAX_INPUT_TOKENS_PER_REQUEST_MIN: Final[int] = 1
MAX_INPUT_TOKENS_PER_REQUEST_MAX: Final[int] = 10_000_000

MAX_TOOL_CALLS_MIN: Final[int] = 1
MAX_TOOL_CALLS_MAX: Final[int] = 10_000

MAX_REQUESTS_DEFAULT: Final[int] = 50
"""``UsageLimits.request_limit``'s own default, substituted by
:meth:`~pydantic_ai.Agent.iter` whenever loom passes no ``usage_limits``.
Every run has always carried this bound; this constant publishes the number
that was already in force."""
MAX_REQUESTS_MIN: Final[int] = 1
MAX_REQUESTS_MAX: Final[int] = 1_000

UnpricedSpendPolicy = Literal["serve", "refuse"]
"""What a run does when ``max_usd`` is declared but its cost could not be
fully computed. ``serve`` answers with the gap recorded; ``refuse`` fails
the run instead."""

ON_UNPRICED_SPEND_POLICIES: Final[tuple[UnpricedSpendPolicy, ...]] = get_args(UnpricedSpendPolicy)
ON_UNPRICED_SPEND_DEFAULT: Final[UnpricedSpendPolicy] = "serve"

NativeToolName = Literal["web_search", "web_fetch", "code_execution"]
"""Tool the model provider runs in its own infrastructure, by its stable v1 name.

Each name is the ``kind`` the engine's native-tool class reports for itself, so
an artifact names the tool and the engine resolves the class. A name published
here is part of the v1 format forever.
"""

NATIVE_TOOLS: Final[tuple[NativeToolName, ...]] = get_args(NativeToolName)
"""Values ``NativeCapability.tool`` accepts, derived from :data:`NativeToolName`."""

TemplateEngine = Literal["handlebars"]
"""Template engine an instruction block's ``template`` names."""

TEMPLATE_ENGINES: Final[tuple[TemplateEngine, ...]] = get_args(TemplateEngine)
"""Values :attr:`InstructionBlock.template` accepts, derived from :data:`TemplateEngine`."""

_SymbolRef = Annotated[str, msgspec.Meta(pattern=SYMBOL_REF_PATTERN)]
_NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]


class JsonSchemaOutput(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="json_schema",
    tag_field="kind",
):
    """Structured answer described by an inline JSON Schema object.

    Canonical output form: what a generator emits.

    Args:
        schema: JSON Schema object describing the required answer.
    """

    schema: Mapping[str, Any]


class TypeRefOutput(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="type_ref",
    tag_field="kind",
):
    """Structured answer described by an application type.

    Shortcut for hand-written applications; the reference is resolved at
    compile time.

    Args:
        ref: ``module:Symbol`` reference to the answer type.
    """

    ref: _SymbolRef


OutputSpec = JsonSchemaOutput | TypeRefOutput
"""Union of every supported output declaration, tagged on ``kind``."""


class OutputHookSpec(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """Use case the host executes once per completed run, with the validated output.

    The key uses the same vocabulary as :attr:`UsecaseCapability.keys` and is
    resolved against the same registry at compile time. The model never sees
    it: it is not a tool, and it never enters the instructions.

    Args:
        usecase: Use-case key of the registry to execute with the validated output.
    """

    usecase: _NonEmptyStr


class ConversationSpec(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """Use case the host executes before a run that carries a ``conversation_id``.

    It returns the prior history of that conversation as opaque bytes in the
    engine's serialised form, or ``None`` on the first turn. The key uses the
    same vocabulary as :attr:`UsecaseCapability.keys` and is resolved against
    the same registry at compile time. The model never sees it: it is not a
    tool, and it never enters the instructions.

    Args:
        usecase: Use-case key of the registry that loads the prior history.
    """

    usecase: _NonEmptyStr


class UsecaseCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="usecase",
    tag_field="kind",
):
    """Explicitly granted business operations.

    Args:
        keys: Use-case keys granted to the agent. Never expanded automatically.
    """

    keys: Annotated[tuple[_NonEmptyStr, ...], msgspec.Meta(min_length=1)]


class SqlCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="sql",
    tag_field="kind",
):
    """Read-only access to a named SQL connection.

    Result bounds are mandatory: an unbounded query is not representable.

    Args:
        connection:       Named connection; compilation fails unless it is read-only.
        max_rows:         Maximum number of rows a single query may return.
        max_result_bytes: Maximum size of a single query result.
    """

    connection: _NonEmptyStr
    max_rows: Annotated[int, msgspec.Meta(ge=1)]
    max_result_bytes: Annotated[int, msgspec.Meta(ge=1)]


class McpCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="mcp",
    tag_field="kind",
):
    """Tools served by a named remote MCP server.

    The artifact *names* the server; it never locates it. Where the server
    lives, how to authenticate to it and how long to wait are deployment facts
    read from ``ai.mcp_servers``, so the same artifact moves between
    environments unchanged.

    Args:
        server:  Named server, resolved from ``ai.mcp_servers``.
        include: Tool names or glob patterns to expose; empty means all.
        exclude: Tool names or glob patterns to omit, applied after ``include``.
    """

    server: _NonEmptyStr
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()


class SkillsCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="skills",
    tag_field="kind",
):
    """Packaged prompt material from one skill library.

    The artifact *names* a library; it never carries an absolute path.
    ``./name`` resolves beside the artifact and travels with it, a bare name
    resolves against ``ai.skills_root``, and ``..`` is not representable, so a
    library can never escape its own directory.

    Args:
        library: Skill library, either ``./name`` or a bare name.
        include: Skill names or glob patterns to expose; empty means all.
        exclude: Skill names or glob patterns to omit, applied after ``include``.
    """

    library: Annotated[str, msgspec.Meta(min_length=1, pattern=SKILLS_LIBRARY_PATTERN)]
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()


class PythonCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="python",
    tag_field="kind",
):
    """Toolset built by application-owned Python code.

    Args:
        factory: ``module:factory`` called once at build as ``factory(context, **params)``.
            A factory, never a constructed object.
        params: Nested block passed to the factory as keyword arguments. The
            names are validated against the factory's signature at compile;
            the values are decoded YAML, not validated. Settings, never secrets.
    """

    factory: _SymbolRef
    params: dict[str, Any] = msgspec.field(default_factory=dict)


class A2ACapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="a2a",
    tag_field="kind",
):
    """Delegation to a named remote agent reachable over A2A.

    The artifact *names* the agent; ``ai.a2a_agents`` knows where it is and how
    to authenticate to it.

    Args:
        agent:   Named remote agent, resolved from ``ai.a2a_agents``.
        include: Skill names or glob patterns to expose; empty means all.
        exclude: Skill names or glob patterns to omit, applied after ``include``.
    """

    agent: _NonEmptyStr
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()


class NativeCapability(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="native",
    tag_field="kind",
):
    """Tool the model provider executes in its own infrastructure.

    The artifact *names* the tool; whether the model bound to ``model_role``
    admits it is a deployment fact checked at compile time, and the provider
    runs it, so no toolset, timeout or credential of loom is involved.

    Args:
        tool: Provider tool, one of :data:`NATIVE_TOOLS`.
    """

    tool: NativeToolName


CapabilitySpec = (
    UsecaseCapability
    | SqlCapability
    | McpCapability
    | SkillsCapability
    | PythonCapability
    | A2ACapability
    | NativeCapability
)
"""Union of every supported capability declaration, tagged on ``kind``."""


class InstructionBlock(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """One authored instruction block.

    A bare string ``instructions`` is sugar for a single unnamed block with no
    ``template``; a sequence of blocks is authored order, projected onto the
    engine in that same order.

    ``dynamic`` is not authored here: it is not cosmetic, it decides what a
    provider may cache, and it follows from whether ``template`` is declared
    — an author who could set it independently could only get it wrong.

    Args:
        text:     Instruction text. Literal unless ``template`` names a
            template engine; with no ``template``, any ``{{`` it contains
            reaches the model unchanged.
        name:     Optional name identifying the block in compilation issues
            and start-up diagnostics; matches :data:`INSTRUCTION_NAME_PATTERN`.
            It never becomes an addressable id on the engine's own side.
        template: Names the template engine ``text`` is written for, one of
            :data:`TEMPLATE_ENGINES`. ``None`` when ``text`` is a literal
            string.
    """

    text: _NonEmptyStr
    name: Annotated[str, msgspec.Meta(pattern=_INSTRUCTION_NAME_DECODE_PATTERN)] | None = None
    template: TemplateEngine | None = None


class PolicySpec(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """Execution limits an agent runs under.

    Ranges are published as module constants and enforced by a later
    compilation phase, so an out-of-range value is reported as a coded issue
    rather than as a decoding failure. See "Spend caps" and "``max_iterations``
    versus ``max_tool_calls``" in ``docs/ai/artifacts.md``.

    Args:
        retries:         Retries a failed tool call, and an answer
            ``output_check`` rejects, inside one run, always. Retries a
            failed provider call across runs, only when the plan holds no
            capability. See :data:`RETRIES_DESCRIPTION` and "``retries``" in
            ``docs/ai/artifacts.md``.
        tool_timeout_ms: Deadline of a single tool call.
        max_iterations:  Maximum ``ToolCallEvent``\\ s loom's own supervisor
            observes over the event stream in one run; see "``max_iterations``
            versus ``max_tool_calls``" in ``docs/ai/artifacts.md`` for how it
            differs from ``max_tool_calls``.
        run_timeout_ms:  Deadline of a whole run.
        max_history_bytes: Ceiling, in bytes, of the history a ``conversation``
            loader may return; a longer one fails the run.
        max_usd:         Cumulative spend ceiling in US dollars for one run,
            including every retried attempt — not for a ``conversation``.
            ``None`` disables the cap. Projects onto
            ``UsageLimits.cost_limit``; see "Spend caps" in
            ``docs/ai/artifacts.md`` for enforcement timing and the
            YAML/JSON precision difference.
        max_total_tokens: Cumulative input-plus-output token ceiling for the
            whole run. ``None`` disables the cap. Projects onto
            ``UsageLimits.total_tokens_limit``; see "Spend caps" in
            ``docs/ai/artifacts.md``.
        max_input_tokens_per_request: Ceiling on the input tokens of any one
            request in the run. ``None`` disables the cap. Projects onto
            ``UsageLimits.per_request_input_tokens_limit``; see "Spend caps"
            in ``docs/ai/artifacts.md``.
        max_tool_calls:  Cumulative successful tool-call ceiling for the whole
            run. ``None`` disables the cap. Projects onto
            ``UsageLimits.tool_calls_limit``; see "Spend caps" in
            ``docs/ai/artifacts.md``.
        max_requests:    Cumulative model-request ceiling for the whole run,
            counted by the engine. Defaults to ``MAX_REQUESTS_DEFAULT``.
            Projects onto ``UsageLimits.request_limit``; see "Spend caps" in
            ``docs/ai/artifacts.md``.
        on_unpriced_spend: What a run does when ``max_usd`` is declared and
            at least one of its model responses could not be priced. Inert
            when ``max_usd`` is absent; see "Spend caps" in
            ``docs/ai/artifacts.md``.
    """

    retries: int = RETRIES_DEFAULT
    tool_timeout_ms: int = TOOL_TIMEOUT_MS_DEFAULT
    max_iterations: int = MAX_ITERATIONS_DEFAULT
    run_timeout_ms: int = RUN_TIMEOUT_MS_DEFAULT
    max_history_bytes: int = MAX_HISTORY_BYTES_DEFAULT
    max_usd: Decimal | None = None
    max_total_tokens: int | None = None
    max_input_tokens_per_request: int | None = None
    max_tool_calls: int | None = None
    max_requests: int = MAX_REQUESTS_DEFAULT
    on_unpriced_spend: UnpricedSpendPolicy = ON_UNPRICED_SPEND_DEFAULT


class AgentSpecV1(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
):
    """Authored agent definition, format version 1.

    Field order mirrors the published JSON Schema so an artifact reads the same
    way as the contract it validates against.

    Args:
        spec_version:  Format version; always ``1`` for this struct.
        name:          Unique agent name within the application.
        description:   What the agent does. Published in the A2A card.
        deps_type:     Declares the shape of the artifact's state: the
            literal ``dict``, or a ``module:Symbol`` reference matching
            :data:`DEPS_TYPE_PATTERN`. Sugar over ``deps_schema``.
            ``dict`` contains no colon, so the two forms cannot collide.
            ``None`` when the artifact declares no state.
        deps_schema:   Declares the shape of the artifact's state directly,
            as a JSON Schema object — the canonical form of the one
            mechanism ``deps_type`` is sugar over. ``None`` when the
            artifact declares no state, or declares it through ``deps_type``.
        instructions:  Instructions the agent follows: a literal string, or a
            non-empty sequence of :class:`InstructionBlock` in authored
            order. Never published, and never a place to encode
            authorization.
        model_role:    Logical model role bound to a concrete provider and
            model by deployment configuration.
        output:        Declaration of the structured answer the agent returns.
        output_check:  ``module:symbol`` reference to an
            :data:`~loom.ai.abc.OutputCheck`, matching
            :data:`SYMBOL_REF_PATTERN`. Resolved at compile time; ``None``
            when the artifact declares no check.
        on_output:     Use case executed once per completed run with the
            validated output; ``None`` when the artifact declares no hook.
        conversation:  Use case executed before a run that carries a
            ``conversation_id`` to load the prior history; ``None`` when the
            artifact declares no loader.
        capabilities:  Explicitly granted capabilities; empty by default.
        policies:      Execution limits; documented defaults when omitted.
        metadata:      Free-form string labels carried alongside the agent.
    """

    spec_version: Annotated[int, msgspec.Meta(ge=SPEC_VERSION_V1, le=SPEC_VERSION_V1)]
    name: Annotated[str, msgspec.Meta(pattern=AGENT_NAME_PATTERN)]
    description: _NonEmptyStr
    deps_type: Annotated[str, msgspec.Meta(pattern=DEPS_TYPE_PATTERN)] | None = None
    deps_schema: Mapping[str, Any] | None = None
    instructions: _NonEmptyStr | Annotated[tuple[InstructionBlock, ...], msgspec.Meta(min_length=1)]
    model_role: Annotated[str, msgspec.Meta(pattern=MODEL_ROLE_PATTERN)] = DEFAULT_MODEL_ROLE
    output: OutputSpec
    output_check: _SymbolRef | None = None
    on_output: OutputHookSpec | None = None
    conversation: ConversationSpec | None = None
    capabilities: tuple[CapabilitySpec, ...] = ()
    policies: PolicySpec = msgspec.field(default_factory=PolicySpec)
    metadata: Mapping[str, str] = msgspec.field(default_factory=dict)
