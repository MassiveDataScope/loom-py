from __future__ import annotations

from collections.abc import Callable, Collection
from dataclasses import dataclass
from typing import Any

from loom.core.engine.kinds import AGENT, MCP
from loom.core.execution import Kind
from loom.core.persistence.stores import DEFAULT_STORE, StoreCatalog, StoreKey
from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.errors import JoinedAutocommitStore, UncomposedWriteStore, UseCaseErrorCode
from loom.core.use_case.factory import ModelRead
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
class ExecutionBinding:
    """One parameter of an execution kind, resolved per execution through its kind's resolver.

    A plan holds those of ``execute``.

    Args:
        name: Parameter name as declared in the signature.
        kind: The kind whose resolver builds the value.
        declaration: The parameter's default, the marker the resolver reads.
        annotation: Resolved annotation of the parameter.
    """

    name: str
    kind: Kind
    declaration: Any
    annotation: Any


@dataclass(frozen=True)
class AgentBinding:
    """One ``Agent(name)`` marker parameter declared in ``execute``.

    Several may appear in one signature. ``annotation`` is the parameter's
    resolved type hint, an ``AgentHandle[...]`` alias in practice.

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

    Several may appear in one signature.

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
        execution_bindings: Parameters resolved through an execution kind
            (``Agent()``, ``Mcp()``), in declaration order.
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
        primary: The store the use case writes to: the ``__store__`` of its
            main model, else its own ``__store__``, else the default store.
        joined: The further stores it writes to, those of its ``Joined[...]``
            repositories other than ``primary``.

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
    execution_bindings: tuple[ExecutionBinding, ...] = ()
    query_binding: QueryBinding | None = None
    primary: StoreKey = DEFAULT_STORE
    joined: frozenset[StoreKey] = frozenset()

    def model_reads(self) -> tuple[ModelRead, ...]:
        """The model each ``LoadById``, ``Load`` and ``Exists`` step reads, loads first."""
        steps: tuple[LoadStep | ExistsStep, ...] = (*self.load_steps, *self.exists_steps)
        return tuple(ModelRead(self.use_case_type, step.name, step.entity_type) for step in steps)

    def refuse_autocommit_joined(self, transactional: Callable[[StoreKey], bool]) -> None:
        """Refuse a joined store that cannot roll back.

        Args:
            transactional: Whether a store's unit of work can roll back.

        Raises:
            JoinedAutocommitStore: For the first joined store, by name, that cannot.
        """
        for key in sorted(self.joined):
            if not transactional(key):
                issue = UseCaseErrorCode.JOINED_AUTOCOMMIT_STORE.issue(
                    component=self.use_case_type, store=key
                )
                raise JoinedAutocommitStore(issue)

    def refuse_unservable(self, catalog: StoreCatalog, factories: Collection[StoreKey]) -> None:
        """Refuse a store this plan writes to that its invocation could not commit.

        A read-only plan writes nowhere; without any unit of work every
        execution runs bare.

        Args:
            catalog: The stores the application declares.
            factories: The stores that have a unit of work.

        Raises:
            ConfigError: When the primary or a joined store is not declared.
            JoinedAutocommitStore: When a joined store is not transactional.
            UncomposedWriteStore: When a store written to has no unit of work.
        """
        name = self.use_case_type.__qualname__
        if self.primary != DEFAULT_STORE:
            catalog.require(self.primary, purpose=f"the __store__ of {name}")
        self.refuse_autocommit_joined(
            lambda key: catalog.require(key, purpose=f"Joined in {name}").capabilities.transactional
        )
        uncomposed = sorted({self.primary, *self.joined} - set(factories))
        if factories and not self.read_only and uncomposed:
            issue = UseCaseErrorCode.UNCOMPOSED_WRITE_STORE.issue(
                component=self.use_case_type, store=uncomposed[0]
            )
            raise UncomposedWriteStore(issue)

    @property
    def agent_bindings(self) -> tuple[AgentBinding, ...]:
        """The ``Agent()`` execution bindings, in declaration order."""
        return tuple(
            AgentBinding(b.name, b.declaration.name, b.annotation)
            for b in self.execution_bindings
            if b.kind is AGENT
        )

    @property
    def mcp_bindings(self) -> tuple[McpBinding, ...]:
        """The ``Mcp()`` execution bindings, in declaration order."""
        return tuple(
            McpBinding(b.name, b.declaration.server, b.declaration.include)
            for b in self.execution_bindings
            if b.kind is MCP
        )
