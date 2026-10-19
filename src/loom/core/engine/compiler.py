from __future__ import annotations

import inspect
import typing
from dataclasses import dataclass, field
from typing import Any

from loom.core.engine.compilable import Compilable
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.metrics import MetricsAdapter
from loom.core.engine.plan import (
    AgentBinding,
    CallerBinding,
    ComputeStep,
    ExecutionPlan,
    ExistsStep,
    InputBinding,
    LoadStep,
    McpBinding,
    ParamBinding,
    QueryBinding,
    RuleStep,
)
from loom.core.logger import LoggerPort, get_logger
from loom.core.model import UnsupportedBoundaryType, loom_type
from loom.core.use_case.markers import (
    LookupKind,
    SourceKind,
    _AgentMarker,
    _CallerMarker,
    _ExistsMarker,
    _InputMarker,
    _LoadByIdMarker,
    _LoadMarker,
    _McpMarker,
    _QueryMarker,
)


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
    agent_bindings: list[AgentBinding] = field(default_factory=list)
    mcp_bindings: list[McpBinding] = field(default_factory=list)
    input_binding: InputBinding | None = None
    caller_binding: CallerBinding | None = None
    query_binding: QueryBinding | None = None


class UseCaseCompiler:
    """Compiles UseCase subclasses into immutable ExecutionPlans at startup.

    Inspects the ``execute`` signature exactly once per class, validates
    structural constraints, and caches the resulting plan.

    No reflection occurs after compilation — the plan drives all runtime
    execution via ``RuntimeExecutor``.

    Args:
        logger: Optional logger. Defaults to the framework logger.
        metrics: Optional metrics adapter. When provided, receives
            ``COMPILE_START`` and ``COMPILE_DONE`` events.

    Example::

        compiler = UseCaseCompiler(metrics=my_adapter)
        plan = compiler.compile(UpdateUserUseCase)
    """

    def __init__(
        self,
        logger: LoggerPort | None = None,
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self._cache: dict[type[Any], ExecutionPlan] = {}
        self._logger = logger or get_logger(__name__)
        self._metrics = metrics

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

    def _emit(self, event: RuntimeEvent) -> None:
        if self._metrics is not None:
            self._metrics.on_event(event)

    def _build_plan(self, use_case_type: type[Compilable]) -> ExecutionPlan:
        uc_name = use_case_type.__qualname__
        self._logger.info(f"[BOOT] Compiling UseCase: {uc_name}", usecase=uc_name)
        self._emit(RuntimeEvent(kind=EventKind.COMPILE_START, use_case_name=uc_name))

        bindings = self._inspect_execute(use_case_type)
        compute_steps = tuple(
            ComputeStep(fn=fn, accepts_context=_fn_accepts_context(fn))
            for fn in use_case_type.computes
        )
        rule_steps = tuple(
            RuleStep(fn=fn, accepts_context=_fn_accepts_context(fn)) for fn in use_case_type.rules
        )

        self._logger.info(
            f"[BOOT]  - Validated {len(compute_steps)} compute steps",
            usecase=uc_name,
        )
        self._logger.info(
            f"[BOOT]  - Validated {len(rule_steps)} rules",
            usecase=uc_name,
        )

        total = (
            len(bindings.load_steps)
            + len(bindings.exists_steps)
            + len(compute_steps)
            + len(rule_steps)
        )
        self._logger.info(
            f"[BOOT]  - ExecutionPlan built ({total} steps)",
            usecase=uc_name,
        )
        self._emit(RuntimeEvent(kind=EventKind.COMPILE_DONE, use_case_name=uc_name))

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
            agent_bindings=tuple(bindings.agent_bindings),
            mcp_bindings=tuple(bindings.mcp_bindings),
            query_binding=bindings.query_binding,
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

        self._validate_marker_refs(use_case_type, bindings)
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

        if isinstance(marker, _QueryMarker):
            self._collect_query(use_case_type, name, annotation, bindings)
            return

        if isinstance(marker, _AgentMarker):
            self._collect_agent(name, annotation, marker, bindings)
            return

        if isinstance(marker, _McpMarker):
            self._collect_mcp(name, marker, bindings)
            return

        if isinstance(marker, _LoadByIdMarker):
            bindings.load_steps.append(self._build_load_by_id_step(name, marker))
            self._log_load_by_id(marker)
            return

        if isinstance(marker, _LoadMarker):
            bindings.load_steps.append(self._build_load_step(name, marker))
            self._log_load(marker)
            return

        if isinstance(marker, _ExistsMarker):
            bindings.exists_steps.append(self._build_exists_step(name, marker))
            self._log_exists(marker)
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
        cmd_name = getattr(annotation, "__name__", repr(annotation))
        self._logger.info(f"[BOOT]  - Detected Input: {cmd_name}")

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
        self._logger.info(f"[BOOT]  - Detected Caller: {name}")

    def _collect_query(
        self,
        use_case_type: type[Compilable],
        name: str,
        annotation: Any,
        bindings: _SignatureBindings,
    ) -> None:
        where = f"{use_case_type.__qualname__}.execute"
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
        self._logger.info(f"[BOOT]  - Detected Query: {annotation.__name__} -> {name}")

    def _collect_agent(
        self,
        name: str,
        annotation: Any,
        marker: _AgentMarker,
        bindings: _SignatureBindings,
    ) -> None:
        """Record one ``Agent()`` marker; several may appear in one signature.

        Only the parameter name, the declared agent name and the raw
        annotation are kept here. Whether that agent exists and whether the
        annotation's type argument matches its declared output cannot be
        checked here: this compiler runs regardless of whether the optional
        AI pillar is even installed, so that check happens once, in the
        composition root, only after the AI runtime it needs has been built.
        """
        bindings.agent_bindings.append(
            AgentBinding(name=name, agent=marker.name, annotation=annotation)
        )
        self._logger.info(f"[BOOT]  - Detected Agent: {marker.name!r} -> {name}")

    def _collect_mcp(
        self,
        name: str,
        marker: _McpMarker,
        bindings: _SignatureBindings,
    ) -> None:
        """Record one ``Mcp()`` marker; several may appear in one signature.

        Only the parameter name, the declared server name and the declared
        ``include`` are kept here. Whether that server exists cannot be
        checked here: this compiler runs regardless of whether the optional
        AI pillar is even installed, the same reason ``_collect_agent``
        cannot validate the agent name either.
        """
        bindings.mcp_bindings.append(
            McpBinding(name=name, server=marker.server, include=marker.include)
        )
        self._logger.info(f"[BOOT]  - Detected Mcp: {marker.server!r} -> {name}")

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
        )

    def _log_load_by_id(self, marker: _LoadByIdMarker[Any]) -> None:
        self._logger.info(
            f"[BOOT]  - Detected LoadById: {marker.entity_type.__name__} by {marker.by}"
        )

    def _log_load(self, marker: _LoadMarker[Any]) -> None:
        src = f"{marker.from_kind.value}:{marker.from_name}"
        self._logger.info(
            f"[BOOT]  - Detected Load: {marker.entity_type.__name__} {marker.against} <- {src}"
        )

    def _log_exists(self, marker: _ExistsMarker[Any]) -> None:
        src = f"{marker.from_kind.value}:{marker.from_name}"
        self._logger.info(
            f"[BOOT]  - Detected Exists: {marker.entity_type.__name__} {marker.against} <- {src}"
        )

    def _validate_marker_refs(
        self,
        use_case_type: type[Compilable],
        bindings: _SignatureBindings,
    ) -> None:
        param_names = {pb.name for pb in bindings.param_bindings}
        has_input = bindings.input_binding is not None
        for ls in bindings.load_steps:
            if ls.source_kind is SourceKind.PARAM and ls.source_name not in param_names:
                raise CompilationError(
                    f"{use_case_type.__qualname__}.execute: "
                    f"LoadById({ls.entity_type.__name__}): "
                    f"parameter '{ls.source_name}' not found in execute signature"
                )
            if ls.source_kind is SourceKind.COMMAND and not has_input:
                raise CompilationError(
                    f"{use_case_type.__qualname__}.execute: "
                    f"LoadById({ls.entity_type.__name__}): from_command requires Input() parameter"
                )

        for es in bindings.exists_steps:
            if es.source_kind is SourceKind.PARAM and es.source_name not in param_names:
                raise CompilationError(
                    f"{use_case_type.__qualname__}.execute: "
                    f"Exists({es.entity_type.__name__}): "
                    f"parameter '{es.source_name}' not found in execute signature"
                )
            if es.source_kind is SourceKind.COMMAND and not has_input:
                raise CompilationError(
                    f"{use_case_type.__qualname__}.execute: "
                    f"Exists({es.entity_type.__name__}): from_command requires Input() parameter"
                )


def _fn_accepts_context(fn: Any) -> bool:
    """Return ``True`` when *fn* accepts a third positional context argument.

    Called once per compute/rule function at compile time so the executor
    never needs to call ``inspect.signature`` on the hot request path.

    Args:
        fn: Callable to introspect.

    Returns:
        ``True`` if *fn* has three or more positional parameters, or uses
        ``*args`` (``VAR_POSITIONAL``).
    """
    try:
        params = tuple(inspect.signature(fn).parameters.values())
    except (TypeError, ValueError):
        return False

    positional_kinds = (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    )
    positional = [p for p in params if p.kind in positional_kinds]
    if len(positional) >= 3:
        return True
    return any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in params)
