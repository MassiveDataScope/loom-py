"""The plan-driven steps that build the arguments of ``execute()``, and the call itself.

Each plan is compiled once, on its first execution, into a table of steps:
only the step kinds the plan declares, in a fixed order — the query check,
the parameters, the caller, the execution kinds (``Agent()``, ``Mcp()``) and
the command bind the arguments; the loads and existence checks fetch from
repositories; the computes and rules shape and check the command. Every step
kind is one small function of the plan returning the step, or ``None`` when
the plan does not declare it. Each step records what it ran on the
execution's :class:`~loom.core.engine.observation.StartedExecution`.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from loom.core.engine.compilable import Compilable
from loom.core.engine.observation import StartedExecution
from loom.core.engine.plan import ExecutionPlan, ExistsStep, LoadStep, ParameterBindingError
from loom.core.errors import MissingCaller, RuleViolationError, RuleViolationsError
from loom.core.execution import ExecutionKinds, UnboundKind
from loom.core.identity import Identity
from loom.core.use_case.kinds import CALLER

if TYPE_CHECKING:
    from loom.core.job.delivery import JobDelivery


@dataclass(frozen=True, slots=True)
class ExecutionInputs:
    """What one execution's caller passed for its arguments, as a single value."""

    params: dict[str, Any] | None = None
    payload: dict[str, Any] | None = None
    dependencies: dict[type[Any], Any] | None = None
    load_overrides: dict[type[Any], Any] | None = None
    delivery: JobDelivery | None = None


@dataclass(slots=True)
class _ExecuteArguments:
    """The arguments of one ``execute()`` call, as the steps build them."""

    inputs: ExecutionInputs
    identity: Identity | None
    started: StartedExecution
    bound: dict[str, Any] = field(default_factory=dict)
    fields_set: frozenset[str] = frozenset()


Bind: TypeAlias = Callable[[_ExecuteArguments], None]
FetchOne: TypeAlias = Callable[[Any, ExecutionPlan, _ExecuteArguments], Awaitable[Any]]
Fetch: TypeAlias = tuple[FetchOne, LoadStep | ExistsStep, str]


@dataclass(slots=True, frozen=True)
class _PlanSteps:
    """One plan's steps by phase, in order: what binds, what fetches, what checks.

    ``bind`` and ``check`` are each one step — the phase's only step, or all
    of them in order — or ``None`` when the plan declares none, so a plan
    with no steps goes straight to ``execute()`` and a one-step phase is a
    direct call. ``fetches`` are rows ``ArgumentBinder.call`` awaits in order.
    """

    plan: ExecutionPlan
    bind: Bind | None
    fetches: tuple[Fetch, ...]
    check: Bind | None


class ArgumentBinder:
    """Builds and makes the ``execute()`` call of each execution of one executor.

    Args:
        kinds: Resolve each ``Agent()`` / ``Mcp()`` parameter.
        repo_resolver: Resolves the repository of an entity type for the
            loads and existence checks the caller passed no dependency for.
    """

    __slots__ = ("_kinds", "_repo_resolver", "_steps_by_use_case")

    def __init__(
        self, kinds: ExecutionKinds, repo_resolver: Callable[[type[Any]], Any] | None
    ) -> None:
        self._kinds = kinds
        self._repo_resolver = repo_resolver
        self._steps_by_use_case: dict[type[Any], _PlanSteps] = {}

    async def call(
        self,
        plan: ExecutionPlan,
        compilable: Compilable,
        inputs: ExecutionInputs,
        identity: Identity | None,
        started: StartedExecution,
    ) -> Any:
        """Run *plan*'s steps over *inputs*, then call *compilable*'s ``execute()`` with them."""
        steps = self._steps_by_use_case.get(plan.use_case_type)
        if steps is None or steps.plan is not plan:
            steps = self._steps_by_use_case[plan.use_case_type] = self._compile(plan)
        call = _ExecuteArguments(inputs, identity, started)
        if steps.bind is not None:
            steps.bind(call)
        if steps.fetches:
            bound, record_step = call.bound, started.step
            for fetch_one, step, label in steps.fetches:
                bound[step.name] = await fetch_one(step, plan, call)
                record_step(label)
        if steps.check is not None:
            steps.check(call)
        started.step("Execute core logic")
        execute = compilable.execute
        if inspect.iscoroutinefunction(execute):
            return await execute(**call.bound)
        return execute(**call.bound)

    def _compile(self, plan: ExecutionPlan) -> _PlanSteps:
        binds = (
            _query(plan),
            _params(plan),
            _caller(plan),
            _delivery(plan),
            self._executions(plan),
            _command(plan),
        )
        fetches: tuple[Fetch, ...] = (
            *((self._load, step, f"Load {step.entity_type.__name__}") for step in plan.load_steps),
            *(
                (self._exist, step, f"Exists {step.entity_type.__name__}")
                for step in plan.exists_steps
            ),
        )
        checks = (_computes(plan), _rules(plan))
        return _PlanSteps(
            plan,
            _combined(binds),
            fetches,
            _combined(checks),
        )

    def _executions(self, plan: ExecutionPlan) -> Bind | None:
        """Inject one value per execution binding, resolved in this execution.

        Requires an identity, as :func:`_caller` does; an explicitly
        anonymous one is accepted and the handle refuses it when used. A kind
        with no resolver bound raises, naming the method that binds it. The
        resolvers read the execution the executor entered, the one the
        constructor was built in.

        Raises:
            MissingCaller: When the execution carried no identity.
        """
        bindings = plan.execution_bindings
        if not bindings:
            return None
        resolve_kind = self._kinds.resolve
        use_case = plan.use_case_type.__qualname__

        def resolve(call: _ExecuteArguments) -> None:
            if call.identity is None:
                first = bindings[0]
                raise MissingCaller(use_case, first.name, first.kind.spelling)
            bound = call.bound
            for binding in bindings:
                try:
                    bound[binding.name] = resolve_kind(
                        binding.kind, binding.declaration, binding.annotation
                    )
                except UnboundKind:
                    raise RuntimeError(
                        f"{use_case}.execute declares an {binding.kind.spelling} parameter but "
                        f"no {binding.kind.name!r} resolver is bound on this executor. Call "
                        f"RuntimeExecutor.bind_{binding.kind.name}_resolver() once the AI "
                        "runtime exists."
                    ) from None

        return resolve

    async def _load(self, step: LoadStep, plan: ExecutionPlan, call: _ExecuteArguments) -> Any:
        overrides = call.inputs.load_overrides
        if overrides and step.entity_type in overrides:
            return overrides[step.entity_type]
        dependencies = call.inputs.dependencies
        repo = None if dependencies is None else dependencies.get(step.entity_type)
        if repo is None:
            repo = self._repository(step.entity_type, dependencies, "LoadById")
        value = step.value(call.bound)
        entity = await step.fetch(repo, value)
        return step.on_missing.answer(step.entity_type, value) if entity is None else entity

    async def _exist(self, step: ExistsStep, plan: ExecutionPlan, call: _ExecuteArguments) -> bool:
        dependencies = call.inputs.dependencies
        repo = None if dependencies is None else dependencies.get(step.entity_type)
        if repo is None:
            repo = self._repository(step.entity_type, dependencies, "Exists")
        value = step.value(call.bound)
        exists_by = getattr(repo, "exists_by", None)
        if not callable(exists_by):
            raise RuntimeError(
                "Repository for "
                f"'{step.entity_type.__name__}' must implement exists_by(field, value)"
            )
        exists_by_async = cast(Callable[[str, Any], Awaitable[bool]], exists_by)
        if await exists_by_async(step.against, value):
            return True
        return bool(step.on_missing.answer(step.entity_type, value))

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


def _combined(steps: tuple[Bind | None, ...]) -> Bind | None:
    """The declared steps of one phase as one step: none, the only one, or all in order."""
    match tuple(step for step in steps if step is not None):
        case ():
            return None
        case (only,):
            return only
        case present:

            def run(call: _ExecuteArguments) -> None:
                for step in present:
                    step(call)

            return run


def _query(plan: ExecutionPlan) -> Bind | None:
    """Refuse a ``Query()`` value that is not an instance of its type, naming the type received."""
    binding = plan.query_binding
    if binding is None:
        return None

    def check(call: _ExecuteArguments) -> None:
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

    def bind(call: _ExecuteArguments) -> None:
        params = call.inputs.params or {}
        for binding in bindings:
            if binding.name not in params:
                raise ValueError(f"{use_case}: missing required parameter '{binding.name}'")
            call.bound[binding.name] = binding.coerce(params[binding.name], use_case)

    return bind


def _caller(plan: ExecutionPlan) -> Bind | None:
    """Inject the declared caller identity; an execution with none is refused.

    Raises:
        MissingCaller: From the step, when the execution carried no identity.
    """
    binding = plan.caller_binding
    if binding is None:
        return None

    def bind(call: _ExecuteArguments) -> None:
        if call.identity is None:
            raise MissingCaller(plan.use_case_type.__qualname__, binding.name, CALLER.spelling)
        call.bound[binding.name] = call.identity

    return bind


def _delivery(plan: ExecutionPlan) -> Bind | None:
    """Inject the delivery the execution was started with, or ``None`` when it had none."""
    binding = plan.delivery_binding
    if binding is None:
        return None

    def bind(call: _ExecuteArguments) -> None:
        call.bound[binding.name] = call.inputs.delivery

    return bind


def _command(plan: ExecutionPlan) -> Bind | None:
    binding = plan.input_binding
    if binding is None:
        return None
    use_case = plan.use_case_type.__qualname__

    def bind(call: _ExecuteArguments) -> None:
        payload = call.inputs.payload
        if payload is None:
            raise ValueError(f"{use_case}: payload is required for a UseCase with Input()")
        command, fields_set = binding.command_type.from_payload(payload)
        call.bound[binding.name] = command
        call.fields_set = cast(frozenset[str], fields_set)
        call.started.step("Bind Input")

    return bind


def _computes(plan: ExecutionPlan) -> Bind | None:
    binding = plan.input_binding
    if binding is None or not plan.compute_steps:
        return None
    computes = tuple(
        (cast(Any, step.fn), step.accepts_context, f"Compute {step.label}")
        for step in plan.compute_steps
    )

    def apply(call: _ExecuteArguments) -> None:
        command = call.bound[binding.name]
        for fn, accepts_context, label in computes:
            if accepts_context:
                command = fn(command, call.fields_set, call.bound)
            else:
                command = fn(command, call.fields_set)
            call.started.step(label)
        call.bound[binding.name] = command

    return apply


def _rules(plan: ExecutionPlan) -> Bind | None:
    binding = plan.input_binding
    if binding is None or not plan.rule_steps:
        return None
    rules = tuple(
        (cast(Any, step.fn), step.accepts_context, step.label, f"Rule {step.label}")
        for step in plan.rule_steps
    )

    def check(call: _ExecuteArguments) -> None:
        command = call.bound[binding.name]
        violations: list[RuleViolationError] = []
        for fn, accepts_context, label, step_label in rules:
            try:
                if accepts_context:
                    fn(command, call.fields_set, call.bound)
                else:
                    fn(command, call.fields_set)
                call.started.step(step_label)
            except RuleViolationError as exc:
                violations.append(exc)
                call.started.rule_failed(label, exc)
        if violations:
            raise RuleViolationsError(violations)

    return check
