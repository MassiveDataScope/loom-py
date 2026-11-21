from __future__ import annotations

import inspect
import logging
import typing
from dataclasses import dataclass, field
from typing import Any, Final, get_args, get_origin

from loom.core.engine.compilable import Compilable
from loom.core.engine.events import EventKind, ExecutionEvent
from loom.core.engine.execution_observer import ExecutionObserver, NullExecutionObserver
from loom.core.engine.log_events import CompilerLogEvent
from loom.core.engine.plan import (
    CallerBinding,
    CommandStep,
    DeliveryBinding,
    ExecutionPlan,
    ExistsStep,
    InputBinding,
    LoadStep,
    ParamBinding,
    QueryBinding,
)
from loom.core.errors import UnsupportedBoundaryType
from loom.core.execution import ExecutionBinding
from loom.core.job.job import Job
from loom.core.logger import LogField
from loom.core.model import BaseModel, loom_type
from loom.core.model.introspection import resolve_type_hints
from loom.core.stores.catalog import StoreKey, store_of
from loom.core.stores.repository.capabilities import (
    BulkCreatable,
    Countable,
    Creatable,
    Deletable,
    FieldReadable,
    Listable,
    Readable,
    RepoFor,
    Updatable,
)
from loom.core.use_case.joined import joined_repository
from loom.core.use_case.kinds import AGENT, MCP
from loom.core.use_case.markers import (
    LookupKind,
    SourceKind,
    _AgentMarker,
    _CallerMarker,
    _DeliveryMarker,
    _ExistsMarker,
    _InputMarker,
    _LoadByIdMarker,
    _LoadMarker,
    _McpMarker,
    _QueryMarker,
)

_log = logging.getLogger(__name__)


class CompilationError(Exception):
    """Raised when a UseCase fails structural validation at startup.

    Args:
        message: Human-readable description of the compilation failure.
    """


@dataclass
class _SignatureBindings:
    """Mutable accumulator filled while walking an ``execute`` signature.

    Keeps the per-parameter dispatch to a single argument instead of one
    positional accumulator per binding kind.
    """

    param_bindings: list[ParamBinding] = field(default_factory=list)
    load_steps: list[LoadStep] = field(default_factory=list)
    exists_steps: list[ExistsStep] = field(default_factory=list)
    execution_bindings: list[ExecutionBinding] = field(default_factory=list)
    input_binding: InputBinding | None = None
    caller_binding: CallerBinding | None = None
    delivery_binding: DeliveryBinding | None = None
    query_binding: QueryBinding | None = None
    detected: list[str] = field(default_factory=list)


class UseCaseCompiler:
    """Compiles UseCase subclasses into immutable ExecutionPlans at startup.

    Inspects the ``execute`` signature exactly once per class, validates
    structural constraints, and caches the resulting plan.

    No reflection occurs after compilation — the plan drives all runtime
    execution via ``UseCaseExecutor``.

    Args:
        execution_observer: Optional execution observer. When provided, receives
            ``COMPILE_START`` and ``COMPILE_DONE`` events.

    Example::

        compiler = UseCaseCompiler(execution_observer=my_adapter)
        plan = compiler.compile(UpdateUserUseCase)
    """

    def __init__(
        self,
        execution_observer: ExecutionObserver | None = None,
    ) -> None:
        self._cache: dict[type[Any], ExecutionPlan] = {}
        self._execution_observer: ExecutionObserver = (
            NullExecutionObserver() if execution_observer is None else execution_observer
        )

    def compile(self, use_case_type: type[Compilable]) -> ExecutionPlan:
        """Return the ExecutionPlan for ``use_case_type``, compiling if needed.

        Compilation is idempotent: calling this method multiple times with
        the same class returns the cached plan without re-inspection.
        Accepts any class satisfying the :class:`~loom.core.engine.compilable.Compilable`
        protocol — both ``UseCase`` and ``Job`` subclasses are valid.

        Args:
            use_case_type: Concrete compilable class to compile.

        Returns:
            Immutable ExecutionPlan.

        Raises:
            CompilationError: If the signature violates structural constraints.
        """
        if use_case_type in self._cache:
            return self._cache[use_case_type]
        plan = self._build_plan(use_case_type)
        self._cache[use_case_type] = plan
        return plan

    def get_plan(self, use_case_type: type[Compilable]) -> ExecutionPlan | None:
        """Return the cached plan for ``use_case_type``, or ``None``.

        Args:
            use_case_type: UseCase subclass to look up.

        Returns:
            Cached ExecutionPlan if compiled, otherwise ``None``.
        """
        return self._cache.get(use_case_type)

    def _build_plan(self, use_case_type: type[Compilable]) -> ExecutionPlan:
        uc_name = use_case_type.__qualname__
        self._execution_observer.on_event(
            ExecutionEvent(kind=EventKind.COMPILE_START, use_case_name=uc_name)
        )

        bindings = self._inspect_execute(use_case_type)
        compute_steps = tuple(CommandStep.from_callable(fn) for fn in use_case_type.computes)
        rule_steps = tuple(CommandStep.from_callable(fn) for fn in use_case_type.rules)

        total = (
            len(bindings.load_steps)
            + len(bindings.exists_steps)
            + len(compute_steps)
            + len(rule_steps)
        )
        _log.info(
            CompilerLogEvent.USE_CASE_COMPILED,
            extra={
                LogField.USE_CASE: uc_name,
                "detected": bindings.detected,
                "computes": len(compute_steps),
                "rules": len(rule_steps),
                "steps": total,
            },
        )
        self._execution_observer.on_event(
            ExecutionEvent(kind=EventKind.COMPILE_DONE, use_case_name=uc_name)
        )

        primary = _primary_store(use_case_type)
        plan = ExecutionPlan(
            use_case_type=use_case_type,
            param_bindings=tuple(bindings.param_bindings),
            input_binding=bindings.input_binding,
            load_steps=tuple(bindings.load_steps),
            exists_steps=tuple(bindings.exists_steps),
            compute_steps=compute_steps,
            rule_steps=rule_steps,
            read_only=bool(getattr(use_case_type, "read_only", False)),
            caller_binding=bindings.caller_binding,
            delivery_binding=bindings.delivery_binding,
            execution_bindings=tuple(bindings.execution_bindings),
            query_binding=bindings.query_binding,
            primary=primary,
            joined=_joined_stores(use_case_type) - {primary},
        )
        use_case_type.__execution_plan__ = plan
        return plan

    def _inspect_execute(self, use_case_type: type[Compilable]) -> _SignatureBindings:
        execute_fn = use_case_type.execute

        if getattr(execute_fn, "__isabstractmethod__", False):
            raise CompilationError(
                f"{use_case_type.__qualname__} must override execute() before it can be compiled"
            )

        try:
            hints = typing.get_type_hints(execute_fn)
        except Exception:
            hints = {}

        bindings = _SignatureBindings()
        for name, param in inspect.signature(execute_fn).parameters.items():
            if self._should_skip_parameter(name, param):
                continue
            self._collect_binding_from_parameter(
                use_case_type=use_case_type,
                name=name,
                param=param,
                annotation=hints.get(name, Any),
                bindings=bindings,
            )

        bindings.load_steps, bindings.exists_steps = self._readable_lookups(use_case_type, bindings)
        return bindings

    def _collect_binding_from_parameter(
        self,
        *,
        use_case_type: type[Compilable],
        name: str,
        param: inspect.Parameter,
        annotation: Any,
        bindings: _SignatureBindings,
    ) -> None:
        marker = param.default

        if isinstance(marker, _InputMarker):
            self._collect_input(use_case_type, name, annotation, bindings)
            return

        if isinstance(marker, _CallerMarker):
            self._collect_caller(use_case_type, name, bindings)
            return

        if isinstance(marker, _DeliveryMarker):
            self._collect_delivery(use_case_type, name, bindings)
            return

        if isinstance(marker, _QueryMarker):
            self._collect_query(use_case_type, name, annotation, bindings)
            return

        if isinstance(marker, _AgentMarker):
            self._collect_execution(ExecutionBinding(name, AGENT, marker, annotation), bindings)
            return

        if isinstance(marker, _McpMarker):
            self._collect_execution(ExecutionBinding(name, MCP, marker, annotation), bindings)
            return

        if isinstance(marker, _LoadByIdMarker):
            bindings.load_steps.append(self._build_load_by_id_step(name, marker))
            bindings.detected.append(f"LoadById: {marker.entity_type.__name__} by {marker.by}")
            return

        if isinstance(marker, _LoadMarker):
            bindings.load_steps.append(self._build_load_step(name, marker))
            bindings.detected.append(
                f"Load: {marker.entity_type.__name__} {marker.against} "
                f"<- {marker.from_kind.value}:{marker.from_name}"
            )
            return

        if isinstance(marker, _ExistsMarker):
            bindings.exists_steps.append(self._build_exists_step(name, marker))
            bindings.detected.append(
                f"Exists: {marker.entity_type.__name__} {marker.against} "
                f"<- {marker.from_kind.value}:{marker.from_name}"
            )
            return

        bindings.param_bindings.append(ParamBinding(name=name, annotation=annotation))

    @staticmethod
    def _should_skip_parameter(name: str, param: inspect.Parameter) -> bool:
        variadic = (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        return name == "self" or param.kind in variadic

    def _collect_input(
        self,
        use_case_type: type[Compilable],
        name: str,
        annotation: Any,
        bindings: _SignatureBindings,
    ) -> None:
        if bindings.input_binding is not None:
            raise CompilationError(
                f"{use_case_type.__qualname__}.execute: only one Input() parameter is allowed"
            )
        from_payload = getattr(annotation, "from_payload", None)
        if not callable(from_payload):
            raise CompilationError(
                f"{use_case_type.__qualname__}.execute: "
                f"Input() parameter '{name}' type must implement from_payload(payload)"
            )
        bindings.input_binding = InputBinding(name=name, command_type=annotation)
        bindings.detected.append(f"Input: {getattr(annotation, '__name__', repr(annotation))}")

    def _collect_caller(
        self,
        use_case_type: type[Compilable],
        name: str,
        bindings: _SignatureBindings,
    ) -> None:
        if bindings.caller_binding is not None:
            raise CompilationError(
                f"{use_case_type.__qualname__}.execute: only one Caller() parameter is allowed"
            )
        bindings.caller_binding = CallerBinding(name=name)
        bindings.detected.append(f"Caller: {name}")

    def _collect_delivery(
        self,
        use_case_type: type[Compilable],
        name: str,
        bindings: _SignatureBindings,
    ) -> None:
        if bindings.delivery_binding is not None:
            raise CompilationError(
                f"{use_case_type.__qualname__}.execute: only one Delivery() parameter is allowed"
            )
        bindings.delivery_binding = DeliveryBinding(name=name)
        bindings.detected.append(f"Delivery: {name}")

    def _collect_query(
        self,
        use_case_type: type[Compilable],
        name: str,
        annotation: Any,
        bindings: _SignatureBindings,
    ) -> None:
        where = f"{use_case_type.__qualname__}.execute"
        if issubclass(use_case_type, Job):
            raise CompilationError(
                f"{where}: a Job cannot declare Query(); a job is dispatched with plain "
                "parameters, so pass the values it needs as parameters instead"
            )
        if bindings.query_binding is not None:
            raise CompilationError(f"{where}: only one Query() parameter is allowed")
        try:
            loom_type(annotation)
        except UnsupportedBoundaryType as exc:
            raise CompilationError(
                f"{where}: Query() parameter '{name}' must be a strict msgspec.Struct or "
                f"pydantic model: {exc.message}"
            ) from exc
        bindings.query_binding = QueryBinding(name=name, query_type=annotation)
        bindings.param_bindings.append(ParamBinding(name=name, annotation=annotation))
        bindings.detected.append(f"Query: {annotation.__name__} -> {name}")

    @staticmethod
    def _collect_execution(binding: ExecutionBinding, bindings: _SignatureBindings) -> None:
        """Record one ``Agent()`` or ``Mcp()`` parameter, detected as its declaration."""
        bindings.execution_bindings.append(binding)
        bindings.detected.append(f"{binding.declaration!r} -> {binding.name}")

    @staticmethod
    def _build_load_by_id_step(name: str, marker: _LoadByIdMarker[Any]) -> LoadStep:
        return LoadStep(
            name=name,
            entity_type=marker.entity_type,
            source_kind=SourceKind.PARAM,
            source_name=marker.by,
            lookup_kind=LookupKind.BY_ID,
            against="id",
            profile=marker.profile,
            on_missing=marker.on_missing,
            spelling=marker.spelling,
        )

    @staticmethod
    def _build_load_step(name: str, marker: _LoadMarker[Any]) -> LoadStep:
        return LoadStep(
            name=name,
            entity_type=marker.entity_type,
            source_kind=marker.from_kind,
            source_name=marker.from_name,
            lookup_kind=LookupKind.BY_FIELD,
            against=marker.against,
            profile=marker.profile,
            on_missing=marker.on_missing,
            spelling=marker.spelling,
        )

    @staticmethod
    def _build_exists_step(name: str, marker: _ExistsMarker[Any]) -> ExistsStep:
        return ExistsStep(
            name=name,
            entity_type=marker.entity_type,
            source_kind=marker.from_kind,
            source_name=marker.from_name,
            against=marker.against,
            on_missing=marker.on_missing,
            spelling=marker.spelling,
        )

    @staticmethod
    def _readable_lookups(
        use_case_type: type[Compilable], bindings: _SignatureBindings
    ) -> tuple[list[LoadStep], list[ExistsStep]]:
        """Return the load and exists steps, each reading its value from a parameter or ``Input()``.

        Raises:
            CompilationError: When a lookup names a missing parameter, or the
                command with no ``Input()`` parameter.
        """
        param_names = {pb.name for pb in bindings.param_bindings}
        command = None if bindings.input_binding is None else bindings.input_binding.name
        for step in (*bindings.load_steps, *bindings.exists_steps):
            problem = step.source_problem(param_names, command is not None)
            if problem is not None:
                raise CompilationError(
                    f"{use_case_type.__qualname__}.execute: "
                    f"{step.spelling} {step.entity_type.__name__}: {problem}"
                )
        return (
            [step.reading(command) for step in bindings.load_steps],
            [step.reading(command) for step in bindings.exists_steps],
        )


def _primary_store(use_case_type: type[Compilable]) -> StoreKey:
    """The ``__store__`` of the use case's main model, else its own, else the default store.

    Raises:
        CompilationError: When it declares ``__store__`` beside a main model.
    """
    model = getattr(use_case_type, "__loom_main_model__", None)
    if model is None:
        return store_of(use_case_type)
    if hasattr(use_case_type, "__store__"):
        raise CompilationError(
            f"{use_case_type.__qualname__} declares __store__ but writes to the store of "
            f"its main model {model.__name__}; remove __store__"
        )
    return store_of(model)


_REPOSITORY_CONTRACTS: Final = frozenset(
    {
        Readable,
        FieldReadable,
        Creatable,
        BulkCreatable,
        Updatable,
        Deletable,
        Listable,
        Countable,
        RepoFor,
    }
)


def _joined_stores(use_case_type: type[Compilable]) -> frozenset[StoreKey]:
    """The stores of the ``Joined[...]`` repositories the constructor injects.

    Raises:
        CompilationError: When a ``Joined[...]`` repository annotation names
            no model, so its store is unknown.
    """
    hints = resolve_type_hints(use_case_type.__init__, include_extras=True)
    stores: set[StoreKey] = set()
    for name, hint in hints.items():
        repository = joined_repository(hint)
        if repository is None:
            continue
        args = get_args(repository)
        model = args[0] if len(args) == 1 else None
        contract = get_origin(repository) in _REPOSITORY_CONTRACTS
        if not (contract and isinstance(model, type) and issubclass(model, BaseModel)):
            raise CompilationError(
                f"{use_case_type.__qualname__}.__init__: {name}: Joined[{repository!r}] "
                "must wrap a repository annotation naming its model, such as "
                "Joined[RepoFor[Model]]"
            )
        stores.add(store_of(model))
    return frozenset(stores)
