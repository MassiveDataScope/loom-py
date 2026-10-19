from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum
from typing import Any, Generic, TypeVar

EntityT = TypeVar("EntityT")


class SourceKind(StrEnum):
    """Origin of a lookup value used by Load/Exists markers."""

    PARAM = "param"
    COMMAND = "command"


class LookupKind(StrEnum):
    """Lookup strategy used by marker-driven prefetch."""

    BY_ID = "by_id"
    BY_FIELD = "by_field"


class OnMissing(StrEnum):
    """Policy applied when a marker lookup does not resolve an entity."""

    RAISE = "raise"
    RETURN_NONE = "return_none"
    RETURN_FALSE = "return_false"


class _InputMarker:
    """Marks a parameter as the command payload input.

    Used to annotate ``execute`` signatures for declarative dispatch.
    Carries no configuration — it is a pure marker.

    Example::

        async def execute(self, command: Input[CreateUser], ...) -> User: ...
    """

    __slots__ = ()


class _CallerMarker:
    """Marks a parameter as the verified identity running the execution.

    Carries no configuration — it is a pure marker, like ``_InputMarker``.

    Example::

        async def execute(self, caller: Identity = Caller()) -> Report: ...
    """

    __slots__ = ()


class _QueryMarker:
    """Marks a parameter as the declared query of the execution.

    Example::

        async def execute(self, query: ListQuery = Query()) -> ItemPage: ...
    """

    __slots__ = ()


class _AgentMarker:
    """Marks a parameter as a named agent handle bound to the verified caller.

    Carries only the agent's name — no configuration and no output type, like
    ``_InputMarker`` and ``_CallerMarker``.  The output type lives on the
    parameter's ``AgentHandle[...]`` annotation, not here: this class must
    never import :mod:`loom.ai`, since that would let a domain-level use-case
    module pull in the AI pillar at import time. The executor resolves the
    name against the compiled agents and hands back a handle typed by the
    annotation the compiler already inspects.

    Example::

        async def execute(
            self,
            caller: Identity = Caller(),
            triage: AgentHandle[SeverityAssessment] = Agent("incident-triage"),
        ) -> IncidentReport: ...
    """

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


class _McpMarker:
    """Marks a parameter as a named MCP server handle bound to this execution.

    Carries only the server name and the tool filter — no output type, like
    ``_AgentMarker``. Unlike ``_AgentMarker``, though, ``McpHandle`` carries
    no type parameter (``loom.ai.abc.McpHandle``): there is no output shape
    for a start-up pass to check the parameter's annotation against, so the
    annotation on this marker's parameter names the protocol only, never a
    generic argument. This class must never import :mod:`loom.ai`, for the
    same containment reason ``_AgentMarker`` gives.

    Example::

        async def execute(
            self,
            caller: Identity = Caller(),
            search: McpHandle = Mcp("docs-server", include=["search", "fetch"]),
        ) -> Report: ...
    """

    __slots__ = ("server", "include")

    def __init__(self, server: str, include: tuple[str, ...]) -> None:
        self.server = server
        self.include = include


class _LoadByIdMarker(Generic[EntityT]):
    """Marks a parameter as a prefetched entity loaded by id.

    The orchestrator resolves the entity before calling ``execute``,
    using ``entity_type`` and the value from a primitive execute parameter.

    Args:
        entity_type: The domain entity type to load.
        by: Name of the primitive execute parameter used as lookup id.
        profile: Loading profile passed to ``repo.get_by_id``.  Controls
            which eager-load options are applied (e.g. joined relations).
            Defaults to ``"default"``.
        on_missing: Missing-entity policy. Defaults to ``OnMissing.RAISE``.

    Example::

        async def execute(
            self,
            command: Input[UpdateUser],
            user: User = LoadById(User, by="user_id"),
        ) -> User: ...
    """

    __slots__ = ("entity_type", "by", "profile", "on_missing")

    def __init__(
        self,
        entity_type: type[EntityT],
        *,
        by: str = "id",
        profile: str = "default",
        on_missing: OnMissing = OnMissing.RAISE,
    ) -> None:
        self.entity_type = entity_type
        self.by = by
        self.profile = profile
        self.on_missing = on_missing


class _LoadMarker(Generic[EntityT]):
    """Marks a parameter as a prefetched entity loaded by an arbitrary field."""

    __slots__ = (
        "entity_type",
        "from_kind",
        "from_name",
        "against",
        "profile",
        "on_missing",
    )

    def __init__(
        self,
        entity_type: type[EntityT],
        *,
        from_kind: SourceKind,
        from_name: str,
        against: str,
        profile: str = "default",
        on_missing: OnMissing = OnMissing.RAISE,
    ) -> None:
        self.entity_type = entity_type
        self.from_kind = from_kind
        self.from_name = from_name
        self.against = against
        self.profile = profile
        self.on_missing = on_missing


class _ExistsMarker(Generic[EntityT]):
    """Marks a parameter as a boolean existence check."""

    __slots__ = ("entity_type", "from_kind", "from_name", "against", "on_missing")

    def __init__(
        self,
        entity_type: type[EntityT],
        *,
        from_kind: SourceKind,
        from_name: str,
        against: str,
        on_missing: OnMissing = OnMissing.RETURN_FALSE,
    ) -> None:
        self.entity_type = entity_type
        self.from_kind = from_kind
        self.from_name = from_name
        self.against = against
        self.on_missing = on_missing


def Input() -> Any:
    """Factory returning the runtime marker for command payload parameters.

    Returned value is intentionally typed as ``Any`` in overloads to avoid
    ``mypy`` default-argument incompatibility in signatures like:
    ``cmd: Command = Input()``.
    """
    return _InputMarker()


def Caller() -> Any:
    """Factory returning the runtime marker for the caller-identity parameter.

    The executor injects the
    :class:`~loom.core.identity.identity.Identity` the transport verified for
    this execution.  It is a declaration, not an ambient read: the identity
    travels with the execution instead of hiding in a global.

    Returned value is intentionally typed as ``Any`` in overloads to avoid
    ``mypy`` default-argument incompatibility in signatures like:
    ``caller: Identity = Caller()``.

    Example::

        async def execute(self, query: QuerySpec, caller: Identity = Caller()) -> Report:
            return await self._reports.for_owner(caller.require_subject(), query)
    """
    return _CallerMarker()


def Query() -> Any:
    """Factory returning the runtime marker for the declared query parameter.

    The parameter's annotation is the whole query contract: a strict
    ``msgspec.Struct`` or strict pydantic model whose fields are each a
    string (UUID, date-time and other string formats included), integer,
    number, boolean, enum or ``Literal``, optionally ``None``. A transport
    builds the typed value from its request; any other caller passes an
    instance under the parameter's name.

    Returned value is intentionally typed as ``Any`` to avoid ``mypy``
    default-argument incompatibility in signatures like:
    ``query: ListQuery = Query()``.

    Example::

        class ListQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
            cursor: str | None = None
            limit: Annotated[int, msgspec.Meta(ge=1, le=200)] = 50

        async def execute(self, query: ListQuery = Query()) -> ItemPage: ...
    """
    return _QueryMarker()


def Agent(name: str) -> Any:
    """Factory returning the runtime marker for a named agent handle parameter.

    The executor resolves *name* against the agents compiled for this
    deployment and injects an ``AgentHandle`` bound to this execution's
    verified caller — the only way a use case reaches an agent (constructor
    injection is not offered for this resource). The output type the handle
    carries is read from the parameter's own ``AgentHandle[...]`` annotation,
    never from this factory, and is checked at start-up against the named
    agent's declared output.

    Returned value is intentionally typed as ``Any`` in overloads to avoid
    ``mypy`` default-argument incompatibility in signatures like:
    ``triage: AgentHandle[SeverityAssessment] = Agent("incident-triage")``.

    Args:
        name: Name of a compiled agent, as declared by its artifact.

    Example::

        async def execute(
            self,
            caller: Identity = Caller(),
            triage: AgentHandle[SeverityAssessment] = Agent("incident-triage"),
        ) -> IncidentReport:
            assessment = await triage.run("Assess this incident.")
            ...
    """
    return _AgentMarker(name)


def Mcp(server: str, *, include: Sequence[str]) -> Any:
    """Factory returning the runtime marker for a named MCP server handle parameter.

    The executor resolves *server* against the MCP servers compiled for
    this deployment and injects an ``McpHandle`` bound to this execution's
    verified caller — the only way a use case reaches an MCP server
    (constructor injection is not offered for this resource). Names in
    *include* are globs, matched by the same ``select_names``/``admits`` the
    model's own toolset filter uses; there is no ``exclude`` in this version
    because no caller has asked for one and a short allow-list already
    expresses every case on the table. Under ``ai.remote_clients: optional``,
    a server tolerated unreachable at start-up still resolves to a handle —
    every call on it fails with ``TOOL_UNAVAILABLE`` instead.

    Unlike :func:`Agent`, no output type is **ever** checked against the
    parameter's annotation: ``McpHandle`` carries no type parameter, so there
    is no declared shape to compare it with. Both checks a ``Mcp()`` marker
    gets are already wired at start-up, aborting the boot rather than waiting
    for a first call: *server* is validated against ``ai.mcp_servers``, naming
    the declaring use case and parameter when it is not configured, and
    *include* is checked against the server's real tool list under the same
    ``startup_timeout_ms`` an agent's own ``mcp`` filter is checked against.
    The second check needs a listing, so under
    ``ai.remote_clients: optional`` a server that never connected is skipped
    rather than failing: a tolerated outage means the filter goes unverified,
    not that it verified clean.

    Returned value is intentionally typed as ``Any`` to avoid ``mypy``
    default-argument incompatibility in signatures like:
    ``search: McpHandle = Mcp("docs-server", include=["search"])``.

    Args:
        server: Name of a configured MCP server, as declared under
            ``ai.mcp_servers``.
        include: Glob patterns naming the tools this handle may call.
            Keyword-only and required: everywhere this include/exclude
            shape is used, an empty ``include`` means "every name" — the
            filter only narrows when it carries at least one pattern — so an
            empty sequence here would
            silently grant the *entire server*, not the handful of tools the
            signature names. ``Mcp()`` raises ``ValueError`` instead of
            widening the grant behind the caller's back. A bare ``str`` is
            rejected the same way: ``str`` satisfies ``Sequence[str]``, so
            ``include="search"`` would type-check yet split into six
            single-character glob patterns at runtime.

    Raises:
        ValueError: If *include* is empty, or is a single string instead of
            a sequence of patterns.

    Example::

        async def execute(
            self,
            caller: Identity = Caller(),
            docs: McpHandle = Mcp("docs-server", include=["search", "fetch"]),
        ) -> Report:
            names = docs.tools()
            ...
    """
    if isinstance(include, str):
        raise ValueError(
            "Mcp() include must be a sequence of glob patterns, not a single "
            f"string; got include={include!r}. Wrap it in a list: "
            f"include=[{include!r}]."
        )
    normalized = tuple(include)
    if not normalized:
        raise ValueError("Mcp() requires a non-empty include")
    return _McpMarker(server, normalized)


def LoadById(
    entity_type: type[EntityT],
    *,
    by: str = "id",
    profile: str = "default",
    on_missing: OnMissing = OnMissing.RAISE,
) -> Any:
    """Factory returning marker for preloaded entity parameters by id.

    Returned value is intentionally typed as ``Any`` in overloads to avoid
    ``mypy`` default-argument incompatibility in signatures like:
    ``entity: User = LoadById(User, by="id")``.

    Args:
        entity_type: Domain entity type the repository should load.
        by: Name of the primitive parameter used as the lookup key.
            Defaults to ``"id"``.
        profile: Loading profile forwarded to ``repo.get_by_id``.
            Defaults to ``"default"``.
        on_missing: Missing-entity policy. Defaults to ``OnMissing.RAISE``.
    """
    return _LoadByIdMarker(
        entity_type,
        by=by,
        profile=profile,
        on_missing=on_missing,
    )


def Load(
    entity_type: type[EntityT],
    *,
    from_param: str | None = None,
    from_command: str | None = None,
    against: str,
    profile: str = "default",
    on_missing: OnMissing = OnMissing.RAISE,
) -> Any:
    """Factory returning marker for preloaded entity parameters by field."""

    if (from_param is None) == (from_command is None):
        raise ValueError("Load() requires exactly one of from_param or from_command")

    if from_param is not None:
        return _LoadMarker(
            entity_type,
            from_kind=SourceKind.PARAM,
            from_name=from_param,
            against=against,
            profile=profile,
            on_missing=on_missing,
        )

    return _LoadMarker(
        entity_type,
        from_kind=SourceKind.COMMAND,
        from_name=from_command or "",
        against=against,
        profile=profile,
        on_missing=on_missing,
    )


def Exists(
    entity_type: type[EntityT],
    *,
    from_param: str | None = None,
    from_command: str | None = None,
    against: str,
    on_missing: OnMissing = OnMissing.RETURN_FALSE,
) -> Any:
    """Factory returning marker for boolean existence checks."""
    if (from_param is None) == (from_command is None):
        raise ValueError("Exists() requires exactly one of from_param or from_command")

    if from_param is not None:
        return _ExistsMarker(
            entity_type,
            from_kind=SourceKind.PARAM,
            from_name=from_param,
            against=against,
            on_missing=on_missing,
        )

    return _ExistsMarker(
        entity_type,
        from_kind=SourceKind.COMMAND,
        from_name=from_command or "",
        against=against,
        on_missing=on_missing,
    )
