"""Plan-level compile-time validators (catalog and temp checks)."""

from __future__ import annotations

from collections.abc import Callable
from enum import Enum
from typing import Any, Protocol, TypeGuard

from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode
from loom.etl.compiler._plan import (
    PipelinePlan,
    ProcessPlan,
    StepPlan,
    visit_pipeline_nodes,
    visit_process_nodes,
)
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source._specs import TableSourceSpec, TempSourceSpec
from loom.etl.runtime.contracts import TableDiscovery

_SCHEMA_MODE_OVERWRITE = "overwrite"


class _TableTargetSpecLike(Protocol):
    table_ref: TableRef
    schema_mode: object


class _TempTargetSpecLike(Protocol):
    temp_name: str
    temp_scope: object


def validate_step_catalog(plan: StepPlan, catalog: TableDiscovery) -> None:
    """Validate a single step's sources and target against the catalog."""
    _validate_step_catalog(plan, catalog, set())


def validate_process_catalog(plan: ProcessPlan, catalog: TableDiscovery) -> None:
    """Validate all steps in a process against the catalog."""
    _walk_process_catalog(plan, catalog, set())


def validate_plan_catalog(plan: PipelinePlan, catalog: TableDiscovery) -> None:
    """Walk the plan validating all table references against the catalog."""
    will_create: set[str] = set()
    visit_pipeline_nodes(
        plan.nodes,
        lambda proc: _walk_process_catalog(proc, catalog, will_create),
        on_parallel_group=lambda plans: _walk_parallel_process_group(plans, catalog, will_create),
    )


def _walk_process_catalog(
    plan: ProcessPlan,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    visit_process_nodes(
        plan.nodes,
        lambda step: _validate_step_catalog(step, catalog, will_create),
        on_parallel_group=lambda plans: _walk_parallel_step_group(plans, catalog, will_create),
    )


def _walk_parallel_process_group(
    plans: tuple[ProcessPlan, ...],
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    snapshot = frozenset(will_create)
    group_creates: set[str] = set()
    for proc in plans:
        proc_creates = set(snapshot)
        _walk_process_catalog(proc, catalog, proc_creates)
        group_creates |= proc_creates - snapshot
    will_create |= group_creates


def _walk_parallel_step_group(
    plans: tuple[StepPlan, ...],
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    snapshot = frozenset(will_create)
    group_creates: set[str] = set()
    for step in plans:
        _validate_step_catalog(step, catalog, set(snapshot))
        _track_overwrite(step, group_creates)
    will_create |= group_creates


def _validate_step_catalog(
    step: StepPlan,
    catalog: TableDiscovery,
    will_create: set[str],
) -> None:
    _check_sources(step, catalog, will_create)
    _check_target(step, catalog, will_create)
    _track_overwrite(step, will_create)


def _check_sources(step: StepPlan, catalog: TableDiscovery, will_create: set[str]) -> None:
    for binding in step.source_bindings:
        spec = binding.spec
        if (
            isinstance(spec, TableSourceSpec)
            and spec.table_ref.ref not in will_create
            and not catalog.exists(spec.table_ref)
        ):
            raise ETLCompilationError.from_issue(
                ETLErrorCode.UNKNOWN_SOURCE_TABLE.issue(
                    component=step.step_type, alias=binding.alias, table_ref=spec.table_ref.ref
                )
            )


def _check_target(step: StepPlan, catalog: TableDiscovery, will_create: set[str]) -> None:
    raw_spec = step.target_binding.spec
    if not _is_table_target_spec(raw_spec):
        return
    spec = raw_spec
    table_ref = spec.table_ref
    if (
        _enum_value(spec.schema_mode) != _SCHEMA_MODE_OVERWRITE
        and table_ref.ref not in will_create
        and not catalog.exists(table_ref)
    ):
        raise ETLCompilationError.from_issue(
            ETLErrorCode.UNKNOWN_TARGET_TABLE.issue(
                component=step.step_type, table_ref=table_ref.ref
            )
        )


def _track_overwrite(step: StepPlan, will_create: set[str]) -> None:
    raw_spec = step.target_binding.spec
    if not _is_table_target_spec(raw_spec):
        return
    spec = raw_spec
    if _enum_value(spec.schema_mode) == _SCHEMA_MODE_OVERWRITE:
        will_create.add(spec.table_ref.ref)


def _is_table_target_spec(spec: object) -> TypeGuard[_TableTargetSpecLike]:
    table_ref = getattr(spec, "table_ref", None)
    schema_mode = getattr(spec, "schema_mode", None)
    if table_ref is None or schema_mode is None:
        return False
    return isinstance(getattr(table_ref, "ref", None), str)


def _enum_value(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, Enum):
        raw = value.value
        if isinstance(raw, str):
            return raw
    return None


def _allow(*_: Any) -> None:
    return None


def _raise_duplicate(step: type, name: str) -> None:
    raise ETLCompilationError.from_issue(
        ETLErrorCode.DUPLICATE_TEMP_NAME.issue(component=step, name=name)
    )


def _raise_mix(step: type, name: str) -> None:
    raise ETLCompilationError.from_issue(
        ETLErrorCode.INVALID_TEMP_APPEND_MIX.issue(component=step, name=name)
    )


_CONFLICT: dict[tuple[bool, bool], Callable[[type, str], None]] = {
    (True, True): _allow,
    (False, False): _raise_duplicate,
    (True, False): _raise_mix,
    (False, True): _raise_mix,
}


def validate_plan_temps(plan: PipelinePlan) -> None:
    """Walk the plan validating temp forward-references and name uniqueness."""
    seen: dict[str, bool] = {}
    visit_pipeline_nodes(
        plan.nodes,
        lambda proc: _walk_process_temps(proc, seen),
        on_parallel_group=lambda plans: _walk_parallel_process_group_temps(plans, seen),
    )


def _walk_process_temps(plan: ProcessPlan, seen: dict[str, bool]) -> None:
    visit_process_nodes(
        plan.nodes,
        lambda step: _validate_step_temps(step, seen),
        on_parallel_group=lambda plans: _walk_parallel_step_group_temps(plans, seen),
    )


def _walk_parallel_process_group_temps(
    plans: tuple[ProcessPlan, ...], seen: dict[str, bool]
) -> None:
    snapshot = dict(seen)
    group_seen: dict[str, bool] = {}
    for proc in plans:
        proc_seen = dict(snapshot)
        _walk_process_temps(proc, proc_seen)
        new_temps = {k: v for k, v in proc_seen.items() if k not in snapshot}
        for name, is_fan_in in new_temps.items():
            if name in group_seen:
                _CONFLICT[(group_seen[name], is_fan_in)](proc.process_type, name)
        group_seen.update(new_temps)
    seen.update(group_seen)


def _walk_parallel_step_group_temps(plans: tuple[StepPlan, ...], seen: dict[str, bool]) -> None:
    snapshot = dict(seen)
    group_seen = dict(snapshot)
    for step in plans:
        _check_temp_sources(step, snapshot)
        _register_step_target(step, group_seen)
    seen.update(group_seen)


def _validate_step_temps(step: StepPlan, seen: dict[str, bool]) -> None:
    _check_temp_sources(step, seen)
    _register_step_target(step, seen)


def _check_temp_sources(step: StepPlan, seen: dict[str, bool]) -> None:
    for binding in step.source_bindings:
        spec = binding.spec
        if isinstance(spec, TempSourceSpec) and spec.temp_name not in seen:
            raise ETLCompilationError.from_issue(
                ETLErrorCode.TEMP_NOT_PRODUCED.issue(
                    component=step.step_type, alias=binding.alias, temp_name=spec.temp_name
                )
            )


def _register_step_target(step: StepPlan, seen: dict[str, bool]) -> None:
    raw_spec = step.target_binding.spec
    if not _is_temp_target_spec(raw_spec):
        return
    spec = raw_spec
    target_kind = _temp_target_kind(spec)
    if target_kind is None:
        return
    name = spec.temp_name
    is_fan_in = target_kind == "fan_in"
    existing = seen.get(name)
    if existing is None:
        seen[name] = is_fan_in
        return
    _CONFLICT[(existing, is_fan_in)](step.step_type, name)


def _temp_target_kind(spec: object) -> str | None:
    if not _is_temp_target_spec(spec):
        return None
    kind = type(spec).__name__
    if kind == "TempSpec":
        return "strict"
    if kind == "TempFanInSpec":
        return "fan_in"
    return None


def _is_temp_target_spec(spec: object) -> TypeGuard[_TempTargetSpecLike]:
    return isinstance(getattr(spec, "temp_name", None), str) and hasattr(spec, "temp_scope")


__all__ = [
    "validate_plan_catalog",
    "validate_plan_temps",
    "validate_process_catalog",
    "validate_step_catalog",
]
