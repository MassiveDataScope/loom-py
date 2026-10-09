"""Capability phase: one handler per ``kind``, dispatched by a map.

Every grant is validated statically and resolved to a handle: the registered
use-case types, the SQL connection config, the imported factory, the configured
remote server.  Artifacts *name*, they never locate, so ``mcp`` and ``a2a``
resolve their name against deployment configuration — which validated the URL
and the credential reference when it was parsed, so nothing is re-checked here.

Skill libraries resolve on the filesystem, offline: the directory is listed and
the include/exclude filter applied at compile, so the plan carries exact skill
names.  Reading a directory is not network access and keeps FR-010 intact.
"""

from __future__ import annotations

import inspect
import unicodedata
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from loom.ai._filters import select_names
from loom.ai._paths import escapes, is_within
from loom.ai.abc import NativeToolSupport
from loom.ai.compiler._plan import (
    CompiledA2ACapability,
    CompiledCapability,
    CompiledMcpCapability,
    CompiledNativeCapability,
    CompiledPythonCapability,
    CompiledRemoteAuth,
    CompiledSkillsCapability,
    CompiledSqlCapability,
    CompiledUsecaseCapability,
)
from loom.ai.config import A2AAgentConfig, AiConfig, McpServerConfig
from loom.ai.declarative import (
    A2ACapability,
    AgentSpecV1,
    CapabilitySpec,
    McpCapability,
    NativeCapability,
    PythonCapability,
    SkillsCapability,
    SqlCapability,
    UsecaseCapability,
)
from loom.ai.errors import (
    AgentCompilationError,
    AgentCompilationIssue,
    a2a_agent_unknown,
    anonymous_with_data_capability,
    capability_empty,
    capability_kind_unsupported,
    mcp_server_unknown,
    native_tool_duplicate,
    native_tool_unsupported,
    python_factory_not_callable,
    python_factory_params_rejected,
    python_factory_unresolvable,
    skills_library_escapes,
    skills_library_invalid,
    skills_name_collision,
    skills_root_missing,
    sql_config_missing,
    sql_connection_not_readonly,
    sql_connection_roles_unbound,
    sql_connection_unknown,
    sql_result_bound_missing,
    usecase_key_unknown,
)
from loom.ai.inference import InferenceTarget
from loom.core.engine.compilable import Compilable
from loom.core.sql.config import SqlConfig, roles_need_identity_binding
from loom.core.symbols import import_symbol
from loom.core.use_case.registry import UseCaseRegistry

# Kinds that read application data or call application/remote code.  An agent
# that opts out of authentication may hold none of them (FR-045a).  ``skills``
# only injects packaged prompt material and ``native`` runs inside the model
# provider, reading no application data and acting under no caller identity, so
# both are exempt.
_DATA_OR_REMOTE_KINDS: Final[frozenset[str]] = frozenset({"usecase", "sql", "python", "mcp", "a2a"})

# A directory is one skill when it holds this manifest; a library is a
# directory of such directories.  Same rule as ``pydantic-ai-harness``.
_SKILL_MANIFEST: Final[str] = "SKILL.md"

_LOCAL_LIBRARY_PREFIX: Final[str] = "./"

_CompileResult = tuple[tuple[CompiledCapability, ...], list[AgentCompilationIssue]]
_HandlerResult = tuple[CompiledCapability | None, list[AgentCompilationIssue]]
_ResolveResult = tuple[Path | None, list[AgentCompilationIssue]]
_McpCompileResult = tuple[CompiledMcpCapability | None, list[AgentCompilationIssue]]
"""Return shape of :func:`compile_mcp_capability`: narrower than the dispatch
map's own ``_HandlerResult`` since every caller of the shared helper — the
agent path and the use-case marker path alike — always gets a
``CompiledMcpCapability`` back, never any other capability kind."""

_FIRST_POSITIONAL: Final[object] = object()
"""Stand-in for the build-time first positional when binding ``params``."""


@dataclass(frozen=True)
class _Context:
    """Deployment inputs one capability handler may consult."""

    component: str
    engine: str
    registry: UseCaseRegistry
    sql: SqlConfig | None
    skills_root: str | None
    mcp_servers: Mapping[str, McpServerConfig]
    a2a_agents: Mapping[str, A2AAgentConfig]
    source_path: str | None
    anonymous: bool
    inference: InferenceTarget | None
    native_tools: NativeToolSupport | None
    model_role: str


def compile_capabilities(
    spec: AgentSpecV1,
    *,
    component: str,
    config: AiConfig,
    registry: UseCaseRegistry,
    sql: SqlConfig | None,
    supported_kinds: frozenset[str],
    inference: InferenceTarget | None = None,
    native_tools: NativeToolSupport | None = None,
    source_path: str | None = None,
) -> _CompileResult:
    """Validate every declared capability and resolve it to a handle.

    Args:
        spec: Decoded artifact whose capabilities are compiled.
        component: Artifact path or agent name the issues point at.
        config: Deployment configuration (engine, skills root, remote servers).
        registry: Use-case registry the ``usecase`` grants resolve against.
        sql: Data-layer configuration; ``None`` fails every ``sql`` grant.
        supported_kinds: Kinds the configured engine serves, as a plain value.
        inference: Model binding of this agent's role; ``native`` grants are
            checked against it and skipped when the role is unbound.
        native_tools: Oracle answering which provider tools a binding admits;
            resolved from the engine by the bootstrap, never imported here.
        source_path: Artifact file, when known; a ``./`` skill library resolves
            beside it and cannot be resolved without it.

    Returns:
        The compiled capabilities and every issue found across all of them.
    """
    context = _Context(
        component=component,
        engine=config.engine,
        registry=registry,
        sql=sql,
        skills_root=config.skills_root,
        mcp_servers=config.mcp_servers,
        a2a_agents=config.a2a_agents,
        source_path=source_path,
        anonymous=_is_anonymous(spec.name, config),
        inference=inference,
        native_tools=native_tools,
        model_role=spec.model_role,
    )
    compiled: list[CompiledCapability] = []
    issues: list[AgentCompilationIssue] = []
    for capability in spec.capabilities:
        kind = str(capability.__struct_config__.tag)
        if kind not in supported_kinds:
            issues.append(capability_kind_unsupported(component, kind, config.engine))
            continue
        if context.anonymous and kind in _DATA_OR_REMOTE_KINDS:
            issues.append(anonymous_with_data_capability(component, kind))
        item, item_issues = _HANDLERS[type(capability)](capability, context)
        issues.extend(item_issues)
        if item is not None:
            compiled.append(item)
    issues.extend(_skill_collision_issues(compiled, component))
    issues.extend(_native_duplicate_issues(compiled, component))
    return tuple(compiled), issues


def _native_duplicate_issues(
    compiled: list[CompiledCapability],
    component: str,
) -> list[AgentCompilationIssue]:
    """Report a provider tool granted more than once to the same agent."""
    seen: set[str] = set()
    issues: list[AgentCompilationIssue] = []
    for capability in compiled:
        if not isinstance(capability, CompiledNativeCapability):
            continue
        if capability.tool in seen:
            issues.append(native_tool_duplicate(component, capability.tool))
            continue
        seen.add(capability.tool)
    return issues


def _skill_collision_issues(
    compiled: list[CompiledCapability],
    component: str,
) -> list[AgentCompilationIssue]:
    """Report skill names granted twice to the same agent by two libraries."""
    owner: dict[str, str] = {}
    issues: list[AgentCompilationIssue] = []
    for capability in compiled:
        if not isinstance(capability, CompiledSkillsCapability):
            continue
        for name in capability.names:
            first = owner.setdefault(name, capability.library)
            if first != capability.library:
                issues.append(skills_name_collision(component, name, first, capability.library))
    return issues


def _is_anonymous(agent_name: str, config: AiConfig) -> bool:
    endpoint = config.endpoints.get(agent_name)
    return endpoint is not None and endpoint.allow_anonymous


def _compile_usecase(capability: UsecaseCapability, context: _Context) -> _HandlerResult:
    issues: list[AgentCompilationIssue] = []
    use_cases: list[type[Compilable]] = []
    for key in capability.keys:
        try:
            use_cases.append(context.registry.resolve(key))
        except KeyError:
            issues.append(usecase_key_unknown(context.component, key))
    if issues:
        return None, issues
    return CompiledUsecaseCapability(keys=capability.keys, use_cases=tuple(use_cases)), []


def _compile_sql(capability: SqlCapability, context: _Context) -> _HandlerResult:
    if context.sql is None:
        return None, [sql_config_missing(context.component)]
    connection = context.sql.connections.get(capability.connection)
    if connection is None:
        return None, [sql_connection_unknown(context.component, capability.connection)]
    issues: list[AgentCompilationIssue] = []
    if not connection.readonly:
        issues.append(sql_connection_not_readonly(context.component, capability.connection))
    binds_roles = not context.anonymous
    if roles_need_identity_binding(connection.allowed_roles, mechanism_binds_roles=binds_roles):
        issues.append(sql_connection_roles_unbound(context.component, capability.connection))
    # Defensive: the v1 struct already makes absent or non-positive bounds
    # unrepresentable, but a spec built through another path must not pass.
    if capability.max_rows < 1 or capability.max_result_bytes < 1:
        issues.append(sql_result_bound_missing(context.component, capability.connection))
    if issues:
        return None, issues
    return (
        CompiledSqlCapability(
            connection=capability.connection,
            config=connection,
            max_rows=capability.max_rows,
            max_result_bytes=capability.max_result_bytes,
        ),
        [],
    )


def compile_mcp_capability(
    server_name: str,
    *,
    include: tuple[str, ...],
    exclude: tuple[str, ...],
    servers: Mapping[str, McpServerConfig],
    component: str,
) -> _McpCompileResult:
    """Resolve a server name and filter into a :class:`CompiledMcpCapability`.

    Shared by the agent ``mcp`` capability (:func:`_compile_mcp`) and the
    use-case ``Mcp()`` marker path (:mod:`loom.rest.fastapi.auto`), so both
    produce an identical compiled capability from identical inputs and the
    agent path's behaviour cannot drift from an independent copy.

    Args:
        server_name: Name the caller declared, looked up in ``servers``.
        include: Tool-name globs to admit; the caller's own filter.
        exclude: Tool-name globs to refuse.
        servers: Every MCP server configured for this deployment.
        component: Artifact or use-case path the returned issue points at.

    Returns:
        The compiled capability, or ``None`` with one
        :func:`~loom.ai.errors.mcp_server_unknown` issue when ``server_name``
        is not in ``servers``.
    """
    server = servers.get(server_name)
    if server is None:
        return None, [mcp_server_unknown(component, server_name)]
    return (
        CompiledMcpCapability(
            server=server_name,
            transport=server.transport,
            url=server.url,
            headers_ref=server.headers_ref,
            auth=_compile_auth(server.auth),
            timeout_ms=server.timeout_ms,
            command=server.command,
            args=server.args,
            env=tuple(sorted((server.env or {}).items())),
            include=include,
            exclude=exclude,
        ),
        [],
    )


def _compile_mcp(capability: McpCapability, context: _Context) -> _HandlerResult:
    return compile_mcp_capability(
        capability.server,
        include=capability.include,
        exclude=capability.exclude,
        servers=context.mcp_servers,
        component=context.component,
    )


def _compile_auth(auth: Mapping[str, str] | None) -> CompiledRemoteAuth | None:
    """Split a validated ``auth`` block into the strategy name and its settings.

    Shared by the ``mcp`` and ``a2a`` grants: both endpoints declare the same
    block.  Configuration already refused a block whose ``kind`` names no
    registered strategy, so the split is the only work left: ``kind`` selects
    the entry point and everything else becomes its keyword arguments.
    """
    if auth is None:
        return None
    return CompiledRemoteAuth(
        kind=auth["kind"],
        settings=tuple((key, value) for key, value in auth.items() if key != "kind"),
    )


def _compile_a2a(capability: A2ACapability, context: _Context) -> _HandlerResult:
    agent = context.a2a_agents.get(capability.agent)
    if agent is None:
        return None, [a2a_agent_unknown(context.component, capability.agent)]
    return (
        CompiledA2ACapability(
            agent=capability.agent,
            url=agent.url,
            headers_ref=agent.headers_ref,
            auth=_compile_auth(agent.auth),
            include=capability.include,
            exclude=capability.exclude,
        ),
        [],
    )


def _library_base(library: str, context: _Context) -> _ResolveResult:
    """Return the directory ``library`` is anchored to, or why it has none."""
    if library.startswith(_LOCAL_LIBRARY_PREFIX):
        if context.source_path is None:
            reason = "a './' library needs a known artifact path"
            return None, [skills_library_invalid(context.component, library, reason)]
        return Path(context.source_path).parent, []
    if context.skills_root is None:
        return None, [skills_root_missing(context.component)]
    return Path(context.skills_root), []


def _resolve_library(library: str, context: _Context) -> _ResolveResult:
    if escapes(library.removeprefix(_LOCAL_LIBRARY_PREFIX)):
        return None, [skills_library_escapes(context.component, library)]
    base, issues = _library_base(library, context)
    if base is None:
        return None, issues
    resolved = base / library.removeprefix(_LOCAL_LIBRARY_PREFIX)
    if not is_within(resolved, base):
        return None, [skills_library_escapes(context.component, library)]
    return resolved.resolve(), []


def _discover_skills(directory: Path) -> tuple[tuple[str, ...], str | None]:
    """List the skills of a library directory, or say why it is not one.

    Replicates the discovery rule of ``pydantic-ai-harness``: an immediate
    child directory holding a ``SKILL.md`` is one skill, named after the
    directory, NFKC-normalised.
    """
    if not directory.exists():
        return (), "the directory does not exist"
    if not directory.is_dir():
        return (), "the path is not a directory"
    if (directory / _SKILL_MANIFEST).is_file():
        return (), "the path is a single skill, not a library of skills"
    names = sorted(
        unicodedata.normalize("NFKC", child.name)
        for child in directory.iterdir()
        if child.is_dir() and (child / _SKILL_MANIFEST).is_file()
    )
    return tuple(names), None


def _compile_skills(capability: SkillsCapability, context: _Context) -> _HandlerResult:
    library = capability.library
    directory, issues = _resolve_library(library, context)
    if directory is None:
        return None, issues
    discovered, reason = _discover_skills(directory)
    if reason is not None:
        return None, [skills_library_invalid(context.component, library, reason)]
    selected = select_names(
        discovered,
        include=capability.include,
        exclude=capability.exclude,
    )
    if not selected:
        return None, [capability_empty(context.component, "skills")]
    return (
        CompiledSkillsCapability(library=library, directory=str(directory), names=selected),
        [],
    )


def _compile_python(capability: PythonCapability, context: _Context) -> _HandlerResult:
    try:
        factory = import_symbol(capability.factory)
    except (ImportError, AttributeError, ValueError) as exc:
        return None, [python_factory_unresolvable(context.component, capability.factory, str(exc))]
    if not callable(factory):
        return None, [python_factory_not_callable(context.component, capability.factory)]
    issue = _rejected_params(factory, capability.params, context.component, capability.factory)
    if issue is not None:
        return None, [issue]
    compiled = CompiledPythonCapability(
        factory_ref=capability.factory, factory=factory, params=capability.params
    )
    return compiled, []


def _rejected_params(
    factory: Callable[..., object], params: Mapping[str, Any], component: str, ref: str
) -> AgentCompilationIssue | None:
    """Return the issue that keeps ``factory(context, **params)`` from binding.

    The context positional is bound alone first, so a factory with no slot for
    it is reported as not a ``ToolsetFactory`` rather than as rejecting the
    ``params``. A callable whose signature cannot be inspected is accepted:
    Python's own call at build reports whatever is wrong.
    """
    try:
        signature = inspect.signature(factory)
    except (ValueError, TypeError):
        return None
    try:
        signature.bind_partial(_FIRST_POSITIONAL)
    except TypeError:
        return python_factory_not_callable(component, ref)
    try:
        signature.bind(_FIRST_POSITIONAL, **params)
    except TypeError as exc:
        return python_factory_params_rejected(component, ref, str(exc))
    return None


def _compile_native(capability: NativeCapability, context: _Context) -> _HandlerResult:
    """Resolve a provider tool against the model bound to the agent's role.

    An unbound role is reported once by the role resolution, so the grant is
    dropped without a second issue.
    """
    if context.inference is None:
        return None, []
    if context.native_tools is None:
        return None, [capability_kind_unsupported(context.component, "native", context.engine)]
    try:
        supported = context.native_tools(context.inference)
    except AgentCompilationError as exc:
        return None, list(exc.issues)
    if capability.tool not in supported:
        return None, [
            native_tool_unsupported(
                context.component,
                tool=capability.tool,
                role=context.model_role,
                provider=context.inference.provider,
                model=context.inference.model,
                supported=sorted(supported),
            )
        ]
    return CompiledNativeCapability(tool=capability.tool), []


# Dispatch map keyed by the declared capability type.  ``Any`` in the handler
# parameter is the dispatch boundary: each handler is statically typed for its
# own capability, and the map guarantees the pairing.
_HANDLERS: Final[Mapping[type[CapabilitySpec], Callable[[Any, _Context], _HandlerResult]]] = {
    UsecaseCapability: _compile_usecase,
    NativeCapability: _compile_native,
    SqlCapability: _compile_sql,
    McpCapability: _compile_mcp,
    SkillsCapability: _compile_skills,
    PythonCapability: _compile_python,
    A2ACapability: _compile_a2a,
}
