"""The plan-driven steps that build the arguments of ``execute()``, and the call itself.

Each plan is compiled once, on its first execution, into a table of steps:
only the step kinds the plan declares, in a fixed order — the query check,
the parameters, the caller, the execution kinds (``Agent()``, ``Mcp()``) and
the command bind the arguments; the loads and existence checks fetch from
repositories; the computes and rules shape and check the command. Every step
kind is one small function of the plan returning the step, or ``None`` when
the plan does not declare it.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TypeAlias, cast

from loom.core.engine.compilable import Compilable
from loom.core.engine.kinds import AGENT, MCP
from loom.core.engine.observation import Observer
from loom.core.engine.plan import ExecutionPlan, ExistsStep, LoadStep
from loom.core.errors import NotFound, RuleViolation, RuleViolations, Unauthenticated
from loom.core.execution import ExecutionKinds, Kind
from loom.core.identity import Identity
from loom.core.use_case.markers import LookupKind, OnMissing, SourceKind

_KIND_WORDING = {
    AGENT: ("Agent()", "agent", "bind_agent_resolver"),
    MCP: ("Mcp()", "MCP", "bind_mcp_resolver"),
}


class ParameterBindingError(ValueError):
    """Raised when a primitive execute parameter cannot be coerced to its declared type."""


@dataclass(frozen=True, slots=True)
class ExecutionInputs:
    """What one execution's caller passed for its arguments, as a single value."""

    params: dict[str, Any] | None = None
    payload: dict[str, Any] | None = None
    dependencies: dict[type[Any], Any] | None = None
    load_overrides: dict[type[Any], Any] | None = None


@dataclass(slots=True)
class _Call:
    """The arguments of one ``execute()`` call, as the steps build them."""

    inputs: ExecutionInputs
    identity: Identity | None
    bound: dict[str, Any] = field(default_factory=dict)
    fields_set: frozenset[str] = frozenset()


Bind: TypeAlias = Callable[[_Call], None]
FetchOne: TypeAlias = Callable[[Any, ExecutionPlan, _Call], Awaitable[Any]]
Fetch: TypeAlias = tuple[FetchOne, LoadStep | ExistsStep, str]


@dataclass(slots=True, frozen=True)
class _Steps:
    """One plan's steps by phase, in order: what binds, what fetches, what checks.

    ``bind`` and ``check`` are each one step — the phase's only step, or all
    of them in order — or ``None`` when the plan declares none, so a plan
    with no steps goes straight to ``execute()`` and a one-step phase is a
    direct call. ``fetches`` are rows ``Preparation.call`` awaits in order.
    """

    plan: ExecutionPlan
    bind: Bind | None
    fetches: tuple[Fetch, ...]
    check: Bind | None


class Preparation:
    """Builds and makes the ``execute()`` call of each execution of one executor.

    Args:
        kinds: Resolve each ``Agent()`` / ``Mcp()`` parameter.
        repo_resolver: Resolves the repository of an entity type for the
            loads and existence checks the caller passed no dependency for.
        observer: Logs each step and each broken rule.
    """

    __slots__ = ("_kinds", "_observer", "_repo_resolver", "_tables")

    def __init__(
        self,
        kinds: ExecutionKinds,
        repo_resolver: Callable[[type[Any]], Any] | None,
        observer: Observer,
    ) -> None:
        self._kinds = kinds
        self._repo_resolver = repo_resolver
        self._observer = observer
        self._tables: dict[type[Any], _Steps] = {}

    async def call(
        self,
        plan: ExecutionPlan,
        compilable: Compilable,
        inputs: ExecutionInputs,
        identity: Identity | None,
    ) -> Any:
        """Run *plan*'s steps over *inputs*, then call *compilable*'s ``execute()`` with them."""
        steps = self._tables.get(plan.use_case_type)
        if steps is None or steps.plan is not plan:
            steps = self._tables[plan.use_case_type] = self._compile(plan)
        call = _Call(inputs, identity)
        if steps.bind is not None:
            steps.bind(call)
        if steps.fetches:
            bound, log_step = call.bound, self._observer.step
            for fetch_one, step, label in steps.fetches:
                bound[step.name] = await fetch_one(step, plan, call)
                log_step(label)
        if steps.check is not None:
            steps.check(call)
        self._observer.step("Execute core logic")
        execute = compilable.execute
        if inspect.iscoroutinefunction(execute):
            return await execute(**call.bound)
        return execute(**call.bound)

    def _compile(self, plan: ExecutionPlan) -> _Steps:
        binds = (
            _query(plan),
            _params(plan),
            _caller(plan),
            self._executions(plan),
            _command(plan, self._observer),
        )
        fetches: tuple[Fetch, ...] = (
            *((self._load, step, f"Load {step.entity_type.__name__}") for step in plan.load_steps),
            *(
                (self._exist, step, f"Exists {step.entity_type.__name__}")
                for step in plan.exists_steps
            ),
        )
        checks = (_computes(plan, self._observer), _rules(plan, self._observer))
        return _Steps(
            plan,
            _phase(binds),
            fetches,
            _phase(checks),
        )

    def _executions(self, plan: ExecutionPlan) -> Bind | None:
        """Inject one value per execution binding, resolved in this execution.

        Requires an identity for the reason :func:`_caller` does. An
        explicitly anonymous one is accepted: the handle refuses it when
        used, right before the network call, not before it is known whether
        this execution makes one. A kind with no resolver bound fails loud,
        naming the method that binds it, instead of handing the raw marker
        to the use case. The resolvers read the execution the executor
        entered, the one the constructor was built in.
        """
        bindings = tuple(
            (binding.name, binding.kind, binding.declaration, binding.annotation)
            for binding in plan.execution_bindings
        )
        if not bindings:
            return None
        kinds = self._kinds
        resolve_kind = kinds.resolve

        def resolve(call: _Call) -> None:
            if call.identity is None:
                marker = _KIND_WORDING[bindings[0][1]][0]
                raise Unauthenticated(
                    f"{plan.use_case_type.__qualname__}.execute declares an {marker} "
                    "parameter but this execution carried no identity. The transport "
                    "must pass identity=... to the executor (pass ANONYMOUS explicitly "
                    "to run without a caller)."
                )
            bound = call.bound
            for name, kind, declaration, annotation in bindings:
                try:
                    bound[name] = resolve_kind(kind, declaration, annotation)
                except KeyError:
                    _refuse_unbound(kinds, plan, kind)
                    raise

        return resolve

    async def _load(self, step: LoadStep, plan: ExecutionPlan, call: _Call) -> Any:
        overrides = call.inputs.load_overrides
        if overrides and step.entity_type in overrides:
            return overrides[step.entity_type]
        dependencies = call.inputs.dependencies
        repo = None if dependencies is None else dependencies.get(step.entity_type)
        if repo is None:
            repo = self._repository(step.entity_type, dependencies, "LoadById")
        value = _lookup_value(step.source_kind, step.source_name, plan, call.bound)
        entity = await _get_entity(repo, step, value)
        return _missing(step, value) if entity is None else entity

    async def _exist(self, step: ExistsStep, plan: ExecutionPlan, call: _Call) -> bool:
        dependencies = call.inputs.dependencies
        repo = None if dependencies is None else dependencies.get(step.entity_type)
        if repo is None:
            repo = self._repository(step.entity_type, dependencies, "Exists")
        value = _lookup_value(step.source_kind, step.source_name, plan, call.bound)
        exists_by = getattr(repo, "exists_by", None)
        if not callable(exists_by):
            raise RuntimeError(
                "Repository for "
                f"'{step.entity_type.__name__}' must implement exists_by(field, value)"
            )
        exists_by_async = cast(Callable[[str, Any], Awaitable[bool]], exists_by)
        if await exists_by_async(step.against, value):
            return True
        if step.on_missing is OnMissing.RAISE:
            raise NotFound(step.entity_type.__name__, id=value)
        return False

    def _repository(
        self, entity_type: type[Any], dependencies: dict[type[Any], Any] | None, marker: str
    ) -> Any:
        """The resolver's repository of *entity_type*, which the caller's dependencies lack."""
        repo = None if self._repo_resolver is None else self._repo_resolver(entity_type)
        if repo is not None:
            return repo
        if dependencies is None and self._repo_resolver is None:
            raise RuntimeError(f"No dependencies provided for {marker}({entity_type.__name__})")
        raise RuntimeError(f"No repository registered for entity type '{entity_type.__name__}'")


def _phase(steps: tuple[Bind | None, ...]) -> Bind | None:
    """The declared steps of one phase as one step: none, the only one, or all in order."""
    match tuple(step for step in steps if step is not None):
        case ():
            return None
        case (only,):
            return only
        case present:

            def run(call: _Call) -> None:
                for step in present:
                    step(call)

            return run


def _query(plan: ExecutionPlan) -> Bind | None:
    """Refuse a ``Query()`` value that is not an instance of its type.

    A generic coercion would call the type with the raw value: a struct
    whose fields are positional would take a dict as its first field. The
    message names the type received, never the value.
    """
    binding = plan.query_binding
    if binding is None:
        return None

    def check(call: _Call) -> None:
        params = call.inputs.params
        if not params or binding.name not in params:
            return
        raw = params[binding.name]
        if not isinstance(raw, binding.query_type):
            raise ParameterBindingError(
                f"{plan.use_case_type.__qualname__}: parameter '{binding.name}' takes a "
                f"{binding.query_type.__name__} instance, got {type(raw).__name__}"
            )

    return check


def _params(plan: ExecutionPlan) -> Bind | None:
    bindings = plan.param_bindings
    if not bindings:
        return None
    use_case = plan.use_case_type.__qualname__

    def bind(call: _Call) -> None:
        params = call.inputs.params or {}
        for binding in bindings:
            if binding.name not in params:
                raise ValueError(f"{use_case}: missing required parameter '{binding.name}'")
            call.bound[binding.name] = _coerce(
                binding.name, binding.annotation, params[binding.name], use_case
            )

    return bind


def _caller(plan: ExecutionPlan) -> Bind | None:
    """Inject the declared caller identity, refusing to invent one.

    Substituting the anonymous identity for a missing one would turn a
    transport that forgot to propagate the caller into a silently
    unauthenticated execution. The transport must say ``ANONYMOUS``
    explicitly for that to happen.
    """
    binding = plan.caller_binding
    if binding is None:
        return None

    def bind(call: _Call) -> None:
        if call.identity is None:
            raise Unauthenticated(
                f"{plan.use_case_type.__qualname__}.execute declares "
                f"'{binding.name}: Identity = Caller()' but this execution carried no "
                "identity. The transport must pass identity=... to the executor "
                "(pass ANONYMOUS explicitly to run without a caller)."
            )
        call.bound[binding.name] = call.identity

    return bind


def _command(plan: ExecutionPlan, observer: Observer) -> Bind | None:
    binding = plan.input_binding
    if binding is None:
        return None
    use_case = plan.use_case_type.__qualname__

    def bind(call: _Call) -> None:
        payload = call.inputs.payload
        if payload is None:
            raise ValueError(f"{use_case}: payload is required for a UseCase with Input()")
        command_type = binding.command_type
        if not hasattr(command_type, "from_payload"):
            raise TypeError(
                f"{use_case}: command type {command_type!r} must implement from_payload()"
            )
        command, fields_set = command_type.from_payload(payload)
        call.bound[binding.name] = command
        call.fields_set = cast(frozenset[str], fields_set)
        observer.step("Bind Input")

    return bind


def _computes(plan: ExecutionPlan, observer: Observer) -> Bind | None:
    binding = plan.input_binding
    if binding is None or not plan.compute_steps:
        return None
    computes = tuple(
        (cast(Any, step.fn), step.accepts_context, f"Compute {_label(step.fn)}")
        for step in plan.compute_steps
    )

    def apply(call: _Call) -> None:
        command = call.bound[binding.name]
        for fn, accepts_context, label in computes:
            if accepts_context:
                command = fn(command, call.fields_set, call.bound)
            else:
                command = fn(command, call.fields_set)
            observer.step(label)
        call.bound[binding.name] = command

    return apply


def _rules(plan: ExecutionPlan, observer: Observer) -> Bind | None:
    binding = plan.input_binding
    if binding is None or not plan.rule_steps:
        return None
    rules = tuple(
        (step.fn, step.accepts_context, _label(step.fn), f"Rule {_label(step.fn)}")
        for step in plan.rule_steps
    )
    use_case = plan.use_case_type.__qualname__

    def check(call: _Call) -> None:
        command = call.bound[binding.name]
        violations: list[RuleViolation] = []
        for fn, accepts_context, label, step_label in rules:
            try:
                if accepts_context:
                    fn(command, call.fields_set, call.bound)
                else:
                    fn(command, call.fields_set)
                observer.step(step_label)
            except RuleViolation as exc:
                violations.append(exc)
                observer.rule_failed(use_case, label, exc)
        if violations:
            raise RuleViolations(violations)

    return check


def _coerce(name: str, annotation: Any, raw: Any, use_case: str) -> Any:
    if annotation is Any or not isinstance(annotation, type) or isinstance(raw, annotation):
        return raw
    try:
        return annotation(raw)
    except (TypeError, ValueError) as exc:
        raise ParameterBindingError(
            f"{use_case}: invalid value for parameter '{name}': "
            f"expected {annotation.__name__}, got {type(raw).__name__} ({raw!r})"
        ) from exc


def _lookup_value(
    source_kind: SourceKind, source_name: str, plan: ExecutionPlan, bound: dict[str, Any]
) -> Any:
    if source_kind is SourceKind.PARAM:
        return bound[source_name]
    if plan.input_binding is None:
        raise RuntimeError("from_command source requires Input() binding")
    command = bound[plan.input_binding.name]
    if not hasattr(command, source_name):
        raise RuntimeError(f"Command '{type(command).__name__}' has no field '{source_name}'")
    return getattr(command, source_name)


async def _get_entity(repo: Any, step: LoadStep, value: Any) -> Any | None:
    if step.lookup_kind is LookupKind.BY_ID:
        return await repo.get_by_id(value, profile=step.profile)
    get_by = getattr(repo, "get_by", None)
    if not callable(get_by):
        raise RuntimeError(
            f"Repository for '{step.entity_type.__name__}' must implement get_by(field, value)"
        )
    get_by_async = cast(Callable[..., Awaitable[Any | None]], get_by)
    return await get_by_async(step.against, value, profile=step.profile)


def _missing(step: LoadStep, value: Any) -> Any:
    """What a load that found no entity binds, by its ``on_missing`` policy."""
    if step.on_missing is OnMissing.RETURN_NONE:
        return None
    if step.on_missing is OnMissing.RETURN_FALSE:
        return False
    raise NotFound(step.entity_type.__name__, id=value)


def _label(fn: object) -> str:
    return getattr(fn, "__name__", type(fn).__name__)


def _refuse_unbound(kinds: ExecutionKinds, plan: ExecutionPlan, kind: Kind) -> None:
    """Raise the "no resolver bound" error when ``kind`` has none."""
    if kinds.bound(kind):
        return
    marker, resolver, method = _KIND_WORDING[kind]
    raise RuntimeError(
        f"{plan.use_case_type.__qualname__}.execute declares an {marker} "
        f"parameter but no {resolver} resolver is bound on this executor. "
        f"Call RuntimeExecutor.{method}() once the AI runtime exists."
    ) from None
