from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.markers import LookupKind, OnMissing, SourceKind
from loom.core.use_case.rule import RuleFn


@dataclass(frozen=True)
class ParamBinding:
    """A primitive parameter declared in ``execute``.

    Represents a positional/keyword argument that is provided by the
    caller at execution time (e.g. ``user_id: int``).

    Args:
        name: Parameter name as declared in the signature.
        annotation: Resolved type annotation.
    """

    name: str
    annotation: type[Any]


@dataclass(frozen=True)
class InputBinding:
    """A command payload parameter marked with ``Input()``.

    The executor builds the command from the raw payload dict and injects
    it under this parameter name.

    Args:
        name: Parameter name as declared in the signature.
        command_type: The ``Command`` subclass to instantiate.
    """

    name: str
    command_type: type[Any]


@dataclass(frozen=True)
class QueryBinding:
    """The declared query parameter marked with ``Query()``.

    The parameter is also an ordinary :class:`ParamBinding`: the executor
    receives the typed value under ``name``. This binding tells a transport
    which type declares the query it must accept and decode.

    Args:
        name: Parameter name as declared in the signature.
        query_type: The strict ``msgspec.Struct`` or pydantic model declaring
            the query.
    """

    name: str
    query_type: type[Any]


@dataclass(frozen=True)
class CallerBinding:
    """The caller-identity parameter marked with ``Caller()``.

    The executor injects the identity the transport verified for this
    execution.  Unlike :class:`ParamBinding`, the value never comes from the
    request payload or the URL, so it cannot be forged by the caller.

    Args:
        name: Parameter name as declared in the signature.
    """

    name: str


@dataclass(frozen=True)
class AgentBinding:
    """One ``Agent(name)`` marker parameter declared in ``execute``.

    The executor resolves *agent* against the compiled agents of this
    deployment and injects a handle bound to this execution's verified
    caller. Unlike :class:`CallerBinding`, several may appear in one
    signature — a use case may reach more than one named agent.

    ``annotation`` is the parameter's own resolved type hint (an
    ``AgentHandle[...]`` generic alias in practice), carried opaquely: this
    module never imports ``loom.ai``, so it neither inspects nor names the
    protocol it belongs to. Only the AI-pillar startup pass — which already
    imports both sides — reads the type argument out of it, to check it
    against the named agent's own declared output.

    Args:
        name: Parameter name as declared in the signature.
        agent: Agent name the marker declared.
        annotation: Resolved annotation of the parameter.
    """

    name: str
    agent: str
    annotation: Any


@dataclass(frozen=True)
class McpBinding:
    """One ``Mcp(server, include=...)`` marker parameter declared in ``execute``.

    The executor resolves *server* against the MCP servers compiled for
    this deployment and injects a handle bound to this execution's verified
    caller, through the resolver ``bind_mcp_resolver`` wires onto the
    executor. A use case carrying this binding fails with ``RuntimeError``
    at its first execution only if that resolver was never bound — naming
    ``bind_mcp_resolver`` in the message. Like :class:`AgentBinding`,
    several may appear in one signature — a use case may reach more than one
    named server.

    The parameter's annotation is not carried here, unlike ``AgentBinding``:
    ``McpHandle`` has no type parameter, so nothing downstream reads a type
    argument out of it, and storing an unread field is the abstraction the
    project's rules forbid.

    Args:
        name: Parameter name as declared in the signature.
        server: MCP server name the marker declared.
        include: Glob patterns naming the tools this handle may call.
    """

    name: str
    server: str
    include: tuple[str, ...]


@dataclass(frozen=True)
class LoadStep:
    """An entity prefetch step marked with ``LoadById`` or ``Load``.

    The executor resolves the entity from a repository before calling
    ``execute``. Missing behavior is controlled by ``on_missing``.

    Args:
        name: Parameter name as declared in the signature.
        entity_type: Domain entity type to load.
        source_kind: Where the lookup value is extracted from.
        source_name: Name of param/command field used as lookup value.
        lookup_kind: Lookup strategy (id or arbitrary field).
        against: Entity field used in repository lookup.
        profile: Loading profile forwarded to ``repo.get_by_id``.
            Defaults to ``"default"``.
        on_missing: Policy when no entity is found.
    """

    name: str
    entity_type: type[Any]
    source_kind: SourceKind
    source_name: str
    lookup_kind: LookupKind
    against: str
    profile: str = "default"
    on_missing: OnMissing = OnMissing.RAISE


@dataclass(frozen=True)
class ExistsStep:
    """A boolean existence check step marked with ``Exists``."""

    name: str
    entity_type: type[Any]
    source_kind: SourceKind
    source_name: str
    against: str
    on_missing: OnMissing = OnMissing.RETURN_FALSE


@dataclass(frozen=True)
class ComputeStep:
    """A compute transformation step.

    Holds a direct reference to the ``ComputeFn`` so the plan is
    self-contained and requires no back-reference to the UseCase class.

    Args:
        fn: The compute function to apply.
        accepts_context: Pre-computed flag; ``True`` when ``fn`` accepts a
            third positional ``context`` argument.  Resolved at compile time
            to avoid per-request signature inspection.
    """

    fn: ComputeFn[Any]
    accepts_context: bool = False


@dataclass(frozen=True)
class RuleStep:
    """A rule validation step.

    Holds a direct reference to the ``RuleFn`` so the plan is
    self-contained and requires no back-reference to the UseCase class.

    Args:
        fn: The rule function to evaluate.
        accepts_context: Pre-computed flag; ``True`` when ``fn`` accepts a
            third positional ``context`` argument.  Resolved at compile time
            to avoid per-request signature inspection.
    """

    fn: RuleFn
    accepts_context: bool = False


@dataclass(frozen=True)
class ExecutionPlan:
    """Immutable compiled representation of a UseCase's execution flow.

    Built once at startup by ``UseCaseCompiler`` and reused for every
    request. No dynamic reflection occurs after compilation.

    Args:
        use_case_type: The ``UseCase`` subclass this plan was compiled from.
        param_bindings: Primitive parameters bound from the caller.
        input_binding: Command payload binding, or ``None`` if absent.
        caller_binding: Caller-identity binding, or ``None`` when the use case
            declares no ``Caller()`` parameter.
        agent_bindings: ``Agent()`` marker bindings, in declaration order;
            empty when the use case declares none.
        mcp_bindings: ``Mcp()`` marker bindings, in declaration order; empty
            when the use case declares none.
        query_binding: ``Query()`` binding, or ``None`` when the use case
            declares no ``Query()`` parameter.
        load_steps: Entity prefetch steps, in declaration order.
        exists_steps: Boolean existence checks, in declaration order.
        compute_steps: Compute transformations, in declaration order.
        rule_steps: Rule validations, in declaration order.
        read_only: When ``True``, the executor skips opening a
            ``UnitOfWork`` transaction for this use case.  Set automatically
            from ``UseCase.read_only`` at compile time.  The HTTP layer may
            also override this at the handler level (e.g. GET routes).

    Example::

        plan = ExecutionPlan(
            use_case_type=UpdateUserUseCase,
            param_bindings=(ParamBinding("user_id", int),),
            input_binding=InputBinding("cmd", UpdateUserCommand),
            load_steps=(
                LoadStep(
                    "user",
                    User,
                    source_kind=SourceKind.PARAM,
                    source_name="user_id",
                    lookup_kind=LookupKind.BY_ID,
                    against="id",
                ),
            ),
            exists_steps=(),
            compute_steps=(),
            rule_steps=(),
        )
    """

    use_case_type: type[Any]
    param_bindings: tuple[ParamBinding, ...]
    input_binding: InputBinding | None
    load_steps: tuple[LoadStep, ...]
    exists_steps: tuple[ExistsStep, ...]
    compute_steps: tuple[ComputeStep, ...]
    rule_steps: tuple[RuleStep, ...]
    read_only: bool = False
    caller_binding: CallerBinding | None = None
    agent_bindings: tuple[AgentBinding, ...] = ()
    mcp_bindings: tuple[McpBinding, ...] = ()
    query_binding: QueryBinding | None = None
