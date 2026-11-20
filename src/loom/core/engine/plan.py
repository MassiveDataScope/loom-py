from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Self, cast

from loom.core.execution import ExecutionBinding
from loom.core.stores.catalog import DEFAULT_STORE, StoreCapabilities, StoreCatalog, StoreKey
from loom.core.stores.uow.scope import Frame
from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.errors import JoinedAutocommitStore, UncomposedWriteStore, UseCaseErrorCode
from loom.core.use_case.factory import ModelRead
from loom.core.use_case.kinds import AGENT, MCP
from loom.core.use_case.markers import LookupKind, OnMissing, SourceKind
from loom.core.use_case.rule import RuleFn

if TYPE_CHECKING:
    from loom.core.stores.uow.coordinator import Participant


class ParameterBindingError(ValueError):
    """Raised when a primitive execute parameter cannot be coerced to its declared type."""


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

    def coerce(self, raw: Any, use_case: str) -> Any:
        """*raw* as the declared type: as is when it already is one or the type is open.

        Raises:
            ParameterBindingError: When the type rejects *raw*.
        """
        annotation = self.annotation
        if annotation is Any or not isinstance(annotation, type) or isinstance(raw, annotation):
            return raw
        try:
            return annotation(raw)
        except (TypeError, ValueError) as exc:
            raise ParameterBindingError(
                f"{use_case}: invalid value for parameter '{self.name}': "
                f"expected {annotation.__name__}, got {type(raw).__name__} ({raw!r})"
            ) from exc


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
class DeliveryBinding:
    """The job-delivery parameter marked with ``Delivery()``.

    Args:
        name: Parameter name as declared in the signature.
    """

    name: str


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


_ABSENT = object()


@dataclass(frozen=True)
class LookupStep:
    """A step that looks an entity up by a value read from a parameter or the command.

    Args:
        name: Parameter name as declared in the signature.
        entity_type: Domain entity type to look up.
        source_kind: Where the lookup value is extracted from.
        source_name: Name of param/command field used as lookup value.
        command: Name of the ``Input()`` parameter a command-sourced value is
            read from; ``None`` for a parameter-sourced one.
        spelling: The marker that declared the step, as written.
    """

    name: str
    entity_type: type[Any]
    source_kind: SourceKind
    source_name: str
    command: str | None = field(default=None, kw_only=True)
    spelling: str = field(default="LoadById()", kw_only=True)

    def source_problem(self, param_names: Collection[str], has_input: bool) -> str | None:
        """Why the lookup value cannot be read from *param_names* or the command, or ``None``."""
        if self.source_kind is SourceKind.PARAM and self.source_name not in param_names:
            return f"parameter '{self.source_name}' not found in execute signature"
        if self.source_kind is SourceKind.COMMAND and not has_input:
            return "from_command requires Input() parameter"
        return None

    def reading(self, command: str | None) -> Self:
        """This step, reading a command-sourced value from the *command* parameter."""
        if self.source_kind is SourceKind.COMMAND:
            return replace(self, command=command)
        return self

    def value(self, bound: dict[str, Any]) -> Any:
        """The lookup value among the arguments *bound* so far.

        Raises:
            RuntimeError: When the command has no field named ``source_name``.
        """
        if self.command is None:
            return bound[self.source_name]
        command = bound[self.command]
        found = getattr(command, self.source_name, _ABSENT)
        if found is _ABSENT:
            raise RuntimeError(
                f"Command '{type(command).__name__}' has no field '{self.source_name}'"
            )
        return found


@dataclass(frozen=True)
class LoadStep(LookupStep):
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

    lookup_kind: LookupKind
    against: str
    profile: str = "default"
    on_missing: OnMissing = OnMissing.RAISE

    async def fetch(self, repo: Any, value: Any) -> Any | None:
        """The entity *repo* holds under *value*, by id or by ``against``; ``None`` if none.

        Raises:
            RuntimeError: When a field lookup's repository has no ``get_by``.
        """
        if self.lookup_kind is LookupKind.BY_ID:
            return await repo.get_by_id(value, profile=self.profile)
        get_by = getattr(repo, "get_by", None)
        if not callable(get_by):
            raise RuntimeError(
                f"Repository for '{self.entity_type.__name__}' must implement get_by(field, value)"
            )
        get_by_async = cast(Callable[..., Awaitable[Any | None]], get_by)
        return await get_by_async(self.against, value, profile=self.profile)


@dataclass(frozen=True)
class ExistsStep(LookupStep):
    """A boolean existence check step marked with ``Exists``."""

    against: str
    on_missing: OnMissing = OnMissing.RETURN_FALSE
    spelling: str = field(default="Exists()", kw_only=True)


@dataclass(frozen=True)
class CommandStep:
    """One compute or rule function applied to the command.

    Args:
        fn: The compute or rule function.
        accepts_context: Whether ``fn`` takes a third positional argument,
            the arguments bound so far.
        label: The function's name, as logs show it.
    """

    fn: ComputeFn[Any] | RuleFn
    accepts_context: bool = False
    label: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "label", getattr(self.fn, "__name__", type(self.fn).__name__))

    @classmethod
    def from_callable(cls, fn: ComputeFn[Any] | RuleFn) -> CommandStep:
        """The step of *fn*, reading once whether it takes the bound arguments.

        A function taking three or more positional parameters, or ``*args``,
        takes them.
        """
        try:
            params = tuple(inspect.signature(fn).parameters.values())
        except (TypeError, ValueError):
            return cls(fn)
        positional = [p for p in params if p.kind in _POSITIONAL]
        variadic = any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in params)
        return cls(fn, len(positional) >= 3 or variadic)


_POSITIONAL = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)

ComputeStep = CommandStep
RuleStep = CommandStep


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
    compute_steps: tuple[CommandStep, ...]
    rule_steps: tuple[CommandStep, ...]
    read_only: bool = False
    caller_binding: CallerBinding | None = None
    delivery_binding: DeliveryBinding | None = None
    execution_bindings: tuple[ExecutionBinding, ...] = ()
    query_binding: QueryBinding | None = None
    primary: StoreKey = DEFAULT_STORE
    joined: frozenset[StoreKey] = frozenset()
    frame: Frame = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "frame", Frame(self.primary, self.joined))

    def model_reads(self) -> tuple[ModelRead, ...]:
        """The model each ``LoadById``, ``Load`` and ``Exists`` step reads, loads first."""
        steps: tuple[LookupStep, ...] = (*self.load_steps, *self.exists_steps)
        return tuple(ModelRead(self.use_case_type, step.name, step.entity_type) for step in steps)

    def refuse_autocommit_joined(
        self, stores: Mapping[StoreKey, Participant | StoreCapabilities]
    ) -> None:
        """Refuse a joined store that cannot roll back.

        Args:
            stores: Each joined store's participant or capabilities, which
                tell whether it can roll back.

        Raises:
            JoinedAutocommitStore: For the first joined store, by name, that cannot.
        """
        for key in self.frame.stores[1:]:
            if not stores[key].is_transactional:
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
            {
                key: catalog.require(key, purpose=f"Joined in {name}").capabilities
                for key in self.frame.stores[1:]
            }
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
