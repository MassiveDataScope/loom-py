"""Compiled agent plan structs (Tier 3 of the data model).

The plan is the only artifact-derived input to every downstream stage: every string
reference whose resolution is possible offline dies at compile, so the plan carries
resolved handles — the registered use-case types, the SQL connection config, the
imported toolset factory — never names.

Secret containment: the plan carries no literal secret.  The
one secret-bearing struct it embeds, :class:`~loom.ai.inference.InferenceTarget`,
redacts its references in ``repr`` and refuses msgspec encoding; the built
:class:`~loom.core.model.LoomType` in :class:`CompiledOutput` is likewise not
msgspec-encodable, so an accidental wire encode of a plan raises instead of
leaking.

The plan is only ever built in memory by the compiler; it is never decoded
from JSON, which is why fields may hold arbitrary runtime handles.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any, ClassVar, Final

import msgspec

from loom.ai.abc import OutputCheck, StateShape
from loom.ai.compiler._fingerprint import plan_fingerprint
from loom.ai.declarative import AgentSpecV1, PolicySpec
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.ai.pricing import ModelPrice
from loom.core.engine.compilable import Compilable
from loom.core.identity import Identity
from loom.core.model import LoomFrozenStruct, LoomType
from loom.core.model.redaction import Secret
from loom.core.sql.abc import RoleNotAllowedError, RolesNotBoundError
from loom.core.sql.config import SqlConnectionConfig
from loom.core.sql.roles import resolve_query_roles


class CompiledOutput(LoomFrozenStruct, frozen=True, kw_only=True):
    """Structured-output contract with a :class:`~loom.core.model.LoomType` built at compile time.

    The boundary type is constructed exactly once, at compile.  For a
    ``msgspec.Struct`` output the decode is strict — unknown fields are
    rejected, so the validated bytes are returned unchanged. For a pydantic
    output ``.type`` is handed to pydantic-ai itself as the
    run's ``output_type``, so pydantic-ai — not loom — validates, retries and
    builds the instance; loom never decodes that answer a second time.

    Attributes:
        schema: JSON Schema object handed to the model.
        loom_type: Boundary type produced at compile, whichever library built
            it (:func:`~loom.core.model.loom_type` for a ``type_ref``,
            :func:`~loom.core.model.msgspec_type` for a ``json_schema``): the
            start-up check of an ``Agent()`` marker reads ``.type``. The
            engine reads it differently per library: for a ``msgspec.Struct``,
            ``ai/engines/pydantic_ai/_output.py`` decodes the model's raw
            bytes through ``.decode_json(bytes)``; for a pydantic model,
            ``.type`` reaches ``Agent.from_spec(output_type=...)`` and the
            engine reads the validated answer straight off
            ``result.output``. ``.to_builtins`` projects either answer to
            JSON-mode builtins once, at the wire.
    """

    schema: Mapping[str, Any]
    loom_type: LoomType


class CompiledUsecaseCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Granted business operations resolved against the use-case registry.

    Attributes:
        keys: Granted use-case keys, carried for the self-description.
        use_cases: Registered use-case types, one per key, in key order.
    """

    kind: ClassVar[str] = "usecase"
    dependency_key: ClassVar[None] = None

    keys: tuple[str, ...]
    use_cases: tuple[type[Compilable], ...]


class CompiledSqlCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Read-only SQL access resolved to its connection configuration.

    Attributes:
        connection: Connection name, carried for the self-description.
        config: Validated read-only connection configuration.
        max_rows: Maximum rows one query may return.
        max_result_bytes: Maximum size of one query result.
    """

    kind: ClassVar[str] = "sql"

    connection: str
    config: SqlConnectionConfig
    max_rows: int
    max_result_bytes: int

    @property
    def dependency_key(self) -> str:
        """The key of this grant's connection in health checks."""
        return f"sql:{self.connection}"

    def roles_of(self, identity: Identity) -> tuple[str, ...]:
        """Resolve one caller's roles on this connection, bound to the caller and never empty.

        Args:
            identity: Verified caller whose roles are resolved.

        Returns:
            The caller's roles allowlisted on this connection.

        Raises:
            AgentRunError: With ``UNAUTHORIZED`` when the caller carries no role
                bound to this connection, or none of its roles is allowlisted.
        """
        try:
            roles = resolve_query_roles(
                identity,
                connection=self.connection,
                roles_bound=True,
                allowed_roles=frozenset(self.config.allowed_roles),
                requested_roles=None,
            )
        except (RolesNotBoundError, RoleNotAllowedError) as exc:
            raise AgentRunError(
                AgentRunErrorCode.UNAUTHORIZED,
                f"the caller may not query the {self.connection!r} connection",
            ) from exc
        if not roles:
            raise AgentRunError(
                AgentRunErrorCode.UNAUTHORIZED,
                f"no role of the caller is allowlisted on the {self.connection!r} connection",
            )
        return roles


class CompiledRemoteAuth(LoomFrozenStruct, frozen=True, kw_only=True):
    """Authentication strategy of one remote endpoint, resolved to name and settings.

    One struct serves both outbound transports, MCP servers and A2A agents, as
    the one registry that builds it does: the strategy contract is
    ``httpx.Auth``, which knows nothing of either protocol.

    ``kind`` is separated from the rest of the ``auth`` block once, here, so the
    engine never re-reads configuration to find out which strategy to build.
    The settings are carried as ordered pairs rather than a mapping because the
    plan is a frozen, hashable value.

    Attributes:
        kind: Strategy name registered in the ``loom.ai.remote_auth`` group.
        settings: The rest of the ``auth`` block, passed to the strategy as
            keyword arguments in declaration order. A repr masks them; the
            strategy receives the text when it is built.
    """

    kind: str
    settings: tuple[tuple[str, Secret[str] | str], ...] = ()


class CompiledMcpCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """MCP server grant, resolved against ``ai.mcp_servers``.

    The artifact names a server; the plan carries the resolved handle — the
    transport with its address or command, the credential reference and the
    deadline — so nothing downstream re-reads configuration.  The URL or the
    command resolves in ``__aenter__``, over the network or by spawning a
    subprocess: it is one of the declared exceptions to "strings die at
    compile", and so is the filter, which is applied against the
    server's real tool list.

    Configuration already proved the fields coherent with the transport, so
    ``url`` is set exactly under ``http`` and ``command`` exactly under
    ``stdio``; the engine narrows by transport, never by field.

    Attributes:
        server: Configured server name, carried for the self-description.
        transport: ``http`` for a remote endpoint, ``stdio`` for a subprocess
            of this worker.
        url: Validated ``https://`` server URL, free of inline credentials;
            ``None`` under ``stdio``.
        headers_ref: Resolved header payload. A repr masks it; the client
            reveals it when the connection opens.
        auth: Named authentication strategy, mutually exclusive with
            ``headers_ref``; ``None`` when the server needs no credential.
        timeout_ms: Deadline of a single call to this server.
        command: Executable that speaks MCP over its stdin/stdout; ``None``
            under ``http``.
        args: Arguments passed to ``command``.
        env: Environment handed to the subprocess as key-sorted pairs, so the
            plan stays a hashable value whatever order the operator wrote.
            Values are masked in a repr and revealed when the process starts.
        include: Tool names or glob patterns to expose; empty means all.
        exclude: Tool names or glob patterns to omit, applied after ``include``.
    """

    kind: ClassVar[str] = "mcp"

    server: str
    transport: str = "http"
    url: str | None = None
    headers_ref: Secret[str] | None = None
    auth: CompiledRemoteAuth | None = None
    timeout_ms: int = 20000
    command: str | None = None
    args: tuple[str, ...] = ()
    env: tuple[tuple[str, Secret[str] | str], ...] = ()
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()

    @property
    def dependency_key(self) -> str:
        """The key of this server's shared session in health checks, by registered name."""
        return f"mcp:{self.server}"


def mcp_connection(capability: CompiledMcpCapability) -> CompiledMcpCapability:
    """Return the connection identity of one ``mcp`` grant, its filters cleared.

    A worker opens one client per MCP connection and every agent granted it
    works over that one client, so what makes two grants the same client is
    every fact the connection is made of — transport, address, credential
    reference, deadline, subprocess command, arguments and environment.
    ``include`` and ``exclude`` are per-agent *views* over the same connection,
    so they are emptied here and applied by the agent's own toolset instead.

    Args:
        capability: Compiled grant of one agent.

    Returns:
        The same grant with ``include`` and ``exclude`` emptied. It is
        hashable, so it doubles as the key a shared client is stored under and
        as the capability that client is built from.

    Example::

        assert mcp_connection(read_only) == mcp_connection(read_write)
    """
    if not capability.include and not capability.exclude:
        return capability
    return msgspec.structs.replace(capability, include=(), exclude=())


class CompiledSkillsCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Skill library resolved to a directory and a selected set of skill names.

    Both globs and the library reference die at compile: the plan carries the
    absolute directory and the exact skill names granted, so the engine loads
    them without re-interpreting the artifact.

    Attributes:
        library: Library as written in the artifact, for the self-description.
        directory: Absolute path the library resolved to.
        names: Selected skill names, alphabetically ordered.
    """

    kind: ClassVar[str] = "skills"
    dependency_key: ClassVar[None] = None

    library: str
    directory: str
    names: tuple[str, ...]


class CompiledPythonCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Application toolset factory resolved to an importable callable.

    Attributes:
        factory_ref: ``module:factory`` reference, for the self-description.
        factory: Imported factory, called once at build as
            ``factory(context, **params)`` with a
            :class:`~loom.ai.abc.ToolsetContext` first.
        params: Keyword arguments the artifact declared for the factory. The
            names bind to the factory's signature (checked at compile); the
            values are decoded YAML carried as-is.
    """

    kind: ClassVar[str] = "python"
    dependency_key: ClassVar[None] = None

    factory_ref: str
    factory: Callable[..., object]
    params: Mapping[str, Any] = {}


class CompiledA2ACapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Remote-agent grant, resolved against ``ai.a2a_agents``.

    The card is fetched in ``__aenter__`` and the filter applied against the
    skills it really advertises — one of the declared exceptions to "strings
    die at compile".

    Attributes:
        agent: Configured agent name, carried for the self-description.
        url: Validated ``https://`` remote agent URL, free of credentials.
        headers_ref: Resolved header payload. A repr masks it; the client
            reveals it when the connection opens.
            Applied to every request the client makes, the card fetch included.
        auth: Named authentication strategy, mutually exclusive with
            ``headers_ref``; ``None`` when the agent needs no credential.
        include: Skill names or glob patterns to expose; empty means all.
        exclude: Skill names or glob patterns to omit, applied after ``include``.
    """

    kind: ClassVar[str] = "a2a"

    agent: str
    url: str
    headers_ref: Secret[str] | None = None
    auth: CompiledRemoteAuth | None = None
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()

    @property
    def dependency_key(self) -> str:
        """The key of this agent's shared client in health checks, by registered name."""
        return f"a2a:{self.agent}"


class CompiledNativeCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Provider tool granted to an agent, already checked against its model.

    The plan carries the stable loom name and not the engine's class: the
    compiler never imports an engine, and the name was validated against the
    binding before reaching here.

    Attributes:
        tool: Provider tool, as :data:`~loom.ai.declarative.NATIVE_TOOLS` names it.
    """

    kind: ClassVar[str] = "native"
    dependency_key: ClassVar[None] = None

    tool: str


CompiledCapability = (
    CompiledUsecaseCapability
    | CompiledSqlCapability
    | CompiledMcpCapability
    | CompiledSkillsCapability
    | CompiledPythonCapability
    | CompiledA2ACapability
    | CompiledNativeCapability
)
"""Union of every compiled capability; each exposes its ``kind`` and handle."""


HOOK_CONTEXT_FIELDS: Final[tuple[str, ...]] = (
    "interaction_id",
    "conversation_id",
    "subject",
    "mechanism",
    "agent",
    "provider",
    "model",
)
"""Run-context names the output hook offers to its use case's Input."""

HOOK_OUTPUT_FIELD: Final[str] = "output"
"""Input name under which the hook nests the validated output."""

HOOK_MESSAGES_FIELD: Final[str] = "messages"
"""Input name under which the hook offers the run's serialised new messages."""

CONVERSATION_CONTEXT_FIELDS: Final[tuple[str, ...]] = (
    "conversation_id",
    "interaction_id",
    "subject",
    "mechanism",
    "agent",
)
"""Run-context names the conversation loader offers to its use case's Input."""


class CompiledUseCaseCall(LoomFrozenStruct, frozen=True, kw_only=True):
    """A use case a run executes, resolved, with the Input names it accepts.

    Attributes:
        step: Name of the step in log lines and errors.
        usecase: Use-case key as written in the artifact, for messages.
        use_case: Registered use-case type, as
            :attr:`CompiledUsecaseCapability.use_cases` carries them.
        accepted: Internal names the Input declares; the run-time filter,
            computed once from ``msgspec.structs.fields``.
    """

    step: ClassVar[str]
    usecase: str
    use_case: type[Compilable]
    accepted: frozenset[str]


class CompiledOutputHook(CompiledUseCaseCall, frozen=True, kw_only=True):
    """Use case executed once per completed run, resolved and proven feedable.

    The compiler proves that every required, user-supplied name of the use
    case's Input is one of :data:`HOOK_OUTPUT_FIELD` or
    :data:`HOOK_CONTEXT_FIELDS`, so the runtime never discovers a missing
    field at the end of a run.
    """

    step: ClassVar[str] = "on_output hook"


class CompiledConversation(CompiledUseCaseCall, frozen=True, kw_only=True):
    """Use case executed before a run that carries a ``conversation_id``.

    The compiler proves that every required, user-supplied name of the use
    case's Input is one of :data:`CONVERSATION_CONTEXT_FIELDS` and that the
    Input declares ``conversation_id``, so the loader always knows which
    conversation to load.
    """

    step: ClassVar[str] = "conversation loader"


class CompiledInstruction(LoomFrozenStruct, frozen=True, kw_only=True):
    """One instruction block in authored order, projected onto the plan.

    A bare-string artifact and a one-block artifact compile to the same
    single-element tuple, so nothing downstream branches on which of the two
    forms an artifact declared.

    Attributes:
        text: Instruction text. Literal unless ``template`` names a template
            engine, matching :attr:`~loom.ai.declarative.InstructionBlock.text`.
        name: Optional block name, carried for compilation issues and
            start-up diagnostics. Never an id the engine can address.
        template: Template engine ``text`` is written for, or ``None`` for a
            literal block that reaches the model unrendered.
    """

    text: str
    name: str | None = None
    template: str | None = None


class AgentPlan(LoomFrozenStruct, frozen=True, kw_only=True):
    """Immutable compiled agent, the only input to every downstream stage.

    Attributes:
        name: Unique agent name within the application.
        description: What the agent does; published in the A2A card.
        instructions: Instruction blocks the agent follows, in authored
            order; never published.
        state: Shape of the artifact's declared state, or ``None`` when the
            artifact declares neither ``deps_type`` nor ``deps_schema``.
        spec_version: Artifact format version, retained for self-description.
        inference: Resolved model binding; one binding, no fallback.
        price: Configured price of the bound model, from ``ai.prices``, or
            ``None`` when the deployment declares none for it.
        output: Structured-output contract with its compiled boundary type.
        output_check: Resolved predicate over the answer the engine parsed,
            when the artifact declares ``output_check``; ``None`` otherwise.
        capabilities: Compiled capabilities with resolved handles.
        policies: Validated execution limits.
        on_output: Output hook, when the artifact declares one.
        conversation: Conversation loader, when the artifact declares one.
        metadata: Free-form string labels carried alongside the agent.
        source_path: Artifact provenance for error messages, when known.
    """

    @property
    def grants_use_cases(self) -> bool:
        """Whether the plan holds a ``usecase`` capability."""
        return any(type(grant) is CompiledUsecaseCapability for grant in self.capabilities)

    @property
    def fingerprint(self) -> str:
        """Stable sha256 of the declared facts that decide this agent's answers.

        Covers the format version, the instructions as compiled, the output
        schema, the output check, the kind and declared facts of every
        capability (use case keys, SQL connection, MCP server, skill library
        and names, Python factory reference, A2A agent, native tool, and the
        ``include``/``exclude`` filters of MCP and A2A) and the bound
        provider, model, output mode and options. It covers what the plan
        declares, not the content of the skills, tools or use cases it names:
        editing a skill's text or a tool's code keeps the fingerprint. Leaves
        out the name, description, metadata, provenance, configured price,
        every policy (each bounds a run and fails it past that bound;
        ``max_history_bytes`` also withholds from the hook a history above
        it; none decides the content of an accepted answer), where a
        capability is served from (URL, directory, credentials, timeouts) and
        the binding's region, endpoint, credentials and streaming flag. The
        output check enters by its import path, so editing the check's body
        keeps the fingerprint.
        """
        return plan_fingerprint(self)

    @classmethod
    def compiled(
        cls,
        spec: AgentSpecV1,
        *,
        instructions: tuple[CompiledInstruction, ...],
        state: StateShape | None,
        inference: InferenceTarget,
        price: ModelPrice | None,
        output: CompiledOutput,
        output_check: OutputCheck | None,
        capabilities: tuple[CompiledCapability, ...],
        on_output: CompiledOutputHook | None,
        conversation: CompiledConversation | None,
        source_path: str | None,
    ) -> AgentPlan:
        """Return the plan of ``spec`` from its compiled parts.

        Args:
            spec: The artifact; supplies name, description, format version,
                policies and metadata.
            instructions: Compiled instruction blocks.
            state: Compiled state shape.
            inference: Resolved model binding.
            price: Configured price of the bound model.
            output: Compiled output contract.
            output_check: Resolved output check.
            capabilities: Compiled capabilities.
            on_output: Compiled output hook.
            conversation: Compiled conversation loader.
            source_path: Artifact provenance.
        """
        return cls(
            name=spec.name,
            description=spec.description,
            instructions=instructions,
            state=state,
            spec_version=spec.spec_version,
            inference=inference,
            price=price,
            output=output,
            output_check=output_check,
            capabilities=capabilities,
            policies=spec.policies,
            on_output=on_output,
            conversation=conversation,
            metadata=MappingProxyType(dict(spec.metadata)),
            source_path=source_path,
        )

    name: str
    description: str
    instructions: tuple[CompiledInstruction, ...]
    state: StateShape | None = None
    spec_version: int
    inference: InferenceTarget
    price: ModelPrice | None = None
    output: CompiledOutput
    output_check: OutputCheck | None = None
    capabilities: tuple[CompiledCapability, ...] = ()
    policies: PolicySpec
    on_output: CompiledOutputHook | None = None
    conversation: CompiledConversation | None = None
    metadata: Mapping[str, str]
    source_path: str | None = None
