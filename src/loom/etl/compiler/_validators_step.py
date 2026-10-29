"""Step-level compile-time validators."""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, cast

import msgspec

from loom.core.model import LoomFrozenStruct
from loom.etl.backends._path_template import extract_template_fields
from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode
from loom.etl.compiler._plan import (
    AgentBinding,
    ConfigValueBinding,
    SourceBinding,
    TargetBinding,
)
from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.expr._predicate import PredicateNode
from loom.etl.declarative.source import FileSourceSpec
from loom.etl.declarative.target import TargetSpec
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._table import UpdateSpec, UpsertSpec, upsert_delete_flag_conflict
from loom.etl.pipeline._step_client import ClientStep
from loom.etl.pipeline._step_sql import StepSQL

_log = logging.getLogger(__name__)


class _BinaryPredicateLike(Protocol):
    left: Any
    right: Any


class _InPredicateLike(Protocol):
    ref: Any
    values: Any


class _UnaryPredicateLike(Protocol):
    operand: Any


class _ReplaceWhereSpecLike(Protocol):
    replace_predicate: PredicateNode


class _UpsertSpecLike(Protocol):
    """Merge-shaped spec — satisfied by ``UpsertSpec`` fields and ``UpdateSpec`` aliases."""

    @property
    def upsert_keys(self) -> tuple[str, ...]: ...

    @property
    def partition_cols(self) -> tuple[str, ...]: ...

    @property
    def upsert_exclude(self) -> tuple[str, ...]: ...

    @property
    def upsert_include(self) -> tuple[str, ...]: ...


class _HistorifySpecLike(Protocol):
    keys: tuple[str, ...]
    effective_date: Any
    mode: Any
    track: tuple[str, ...] | None
    overwrite: tuple[str, ...] | None
    valid_from: str
    valid_to: str
    deleted_at: str
    partition_scope: tuple[str, ...] | None
    allow_temporal_rerun: bool
    delete_marker: str | None


class _PredicateShape(Enum):
    BINARY = "binary"
    IN = "in"
    UNARY = "unary"
    UNKNOWN = "unknown"


class _PredicateField(Enum):
    LEFT = "left"
    RIGHT = "right"
    REF = "ref"
    VALUES = "values"
    OPERAND = "operand"


@dataclass(frozen=True)
class StepCompilationContext:
    """Carries resolved step data required by per-step validators."""

    step_type: type[Any]
    params_type: type[Any]
    source_bindings: tuple[SourceBinding, ...]
    target_binding: TargetBinding
    config_bindings: tuple[ConfigValueBinding, ...] = ()
    agent_bindings: tuple[AgentBinding, ...] = ()


def validate_step(ctx: StepCompilationContext) -> None:
    """Validate signature, upsert, history, param expressions, then file paths."""
    validate_execute_signature(
        ctx.step_type,
        ctx.params_type,
        ctx.source_bindings,
        ctx.config_bindings,
        ctx.agent_bindings,
    )
    validate_upsert_spec(ctx.step_type, ctx.target_binding.spec)
    validate_historify_spec(ctx.step_type, ctx.target_binding.spec)
    validate_param_exprs(ctx.step_type, ctx.params_type, ctx.source_bindings, ctx.target_binding)
    validate_file_path_templates(
        ctx.step_type, ctx.params_type, ctx.source_bindings, ctx.target_binding
    )


def validate_execute_signature(
    step_type: type[Any],
    params_type: type[Any],
    source_bindings: tuple[SourceBinding, ...],
    config_bindings: tuple[ConfigValueBinding, ...] = (),
    agent_bindings: tuple[AgentBinding, ...] = (),
) -> None:
    """Validate execute() params and keyword-only source, config and agent bindings."""
    sig = inspect.signature(step_type.execute)
    params = list(sig.parameters.values())
    _validate_params_arg(step_type, params, params_type)
    config_aliases = {b.alias for b in config_bindings}
    agent_aliases = {b.alias for b in agent_bindings}
    if _is_sql_step_type(step_type):
        _check_no_injected_values(step_type, config_aliases, agent_aliases)
        return
    kw_only = _collect_kw_only_frames(params)
    if _is_client_step_type(step_type):
        _check_injected_params(step_type, config_aliases, agent_aliases, kw_only, taken={"client"})
        return
    source_aliases = {b.alias for b in source_bindings}
    _check_injected_params(step_type, config_aliases, agent_aliases, kw_only, taken=source_aliases)
    _check_missing_frames(step_type, source_aliases, kw_only)
    _check_extra_frames(step_type, source_aliases | config_aliases | agent_aliases, kw_only)


def validate_params_compat(
    component_type: type[Any],
    component_params: type[Any],
    context_params: type[Any],
) -> None:
    """Raise if component_params requires fields absent from context_params."""
    if component_params is context_params:
        return
    if issubclass(context_params, component_params):
        return
    component_fields = {f.name for f in msgspec.structs.fields(component_params)}
    context_fields = {f.name for f in msgspec.structs.fields(context_params)}
    missing = component_fields - context_fields
    if missing:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.MISSING_PARAMS_FIELDS.issue(
                component=component_type,
                missing=str(sorted(missing)),
                context=context_params.__name__,
            )
        )


def _validate_params_arg(
    step_type: type[Any],
    params: list[inspect.Parameter],
    params_type: type[Any],
) -> None:
    positional_kinds = {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
    }
    positional = [p for p in params if p.kind in positional_kinds and p.name != "self"]
    if not positional:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.INVALID_PARAMS_TYPE.issue(
                component=step_type, expected=params_type.__name__
            )
        )
    first = positional[0]
    if first.name != "params":
        raise ETLCompilationError.from_issue(
            ETLErrorCode.INVALID_PARAMS_NAME.issue(component=step_type, got=first.name)
        )


def _collect_kw_only_frames(params: list[inspect.Parameter]) -> dict[str, inspect.Parameter]:
    return {p.name: p for p in params if p.kind is inspect.Parameter.KEYWORD_ONLY}


def _is_sql_step_type(step_type: type[Any]) -> bool:
    return issubclass(step_type, StepSQL)


def _is_client_step_type(step_type: type[Any]) -> bool:
    return issubclass(step_type, ClientStep)


def _check_no_injected_values(
    step_type: type[Any], config_aliases: set[str], agent_aliases: set[str]
) -> None:
    if config_aliases:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.UNSUPPORTED_CONFIG_VALUE.issue(component=step_type, marker="FromConfig")
        )
    if agent_aliases:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.UNSUPPORTED_CONFIG_VALUE.issue(component=step_type, marker="WithAgent")
        )


def _check_injected_params(
    step_type: type[Any],
    config_aliases: set[str],
    agent_aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
    *,
    taken: set[str],
) -> None:
    _check_config_params(step_type, config_aliases, kw_only, taken=taken)
    _check_config_params(step_type, agent_aliases, kw_only, taken=taken, marker="WithAgent")


def _check_config_params(
    step_type: type[Any],
    aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
    *,
    taken: set[str],
    marker: str = "FromConfig",
) -> None:
    conflict = aliases & taken
    if conflict:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.CONFIG_ALIAS_CONFLICT.issue(
                component=step_type, marker=marker, aliases=str(sorted(conflict))
            )
        )
    missing = aliases - set(kw_only)
    if missing:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.MISSING_CONFIG_PARAMS.issue(
                component=step_type, marker=marker, missing=str(sorted(missing))
            )
        )


def _check_missing_frames(
    step_type: type[Any],
    source_aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
) -> None:
    missing = source_aliases - set(kw_only)
    if missing:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.MISSING_SOURCE_PARAMS.issue(
                component=step_type, missing=str(sorted(missing))
            )
        )


def _check_extra_frames(
    step_type: type[Any],
    source_aliases: set[str],
    kw_only: dict[str, inspect.Parameter],
) -> None:
    extra = set(kw_only) - source_aliases
    if extra:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.EXTRA_SOURCE_PARAMS.issue(component=step_type, extra=str(sorted(extra)))
        )


def validate_param_exprs(
    step_type: type[Any],
    params_type: type[Any],
    source_bindings: tuple[SourceBinding, ...],
    target_binding: TargetBinding,
) -> None:
    """Raise when any ParamExpr references an undeclared params field.

    A reference is declared when it is a struct field of *params_type* or a
    public ``property`` on the class — computed read-only params resolve
    through the same ``getattr`` chain at runtime as plain fields.
    """
    known = _known_fields(params_type)
    if known is None:
        return
    known = known | _class_properties(params_type)

    exprs: list[ParamExpr] = []
    for binding in source_bindings:
        for pred in getattr(binding.spec, "predicates", ()):
            _collect_exprs(pred, exprs)

    if _is_replace_where_like(target_binding.spec):
        target_spec = cast(_ReplaceWhereSpecLike, target_binding.spec)
        _collect_exprs(target_spec.replace_predicate, exprs)

    if _is_historify_like(target_binding.spec):
        historify_spec = cast(_HistorifySpecLike, target_binding.spec)
        if _is_param_expr(historify_spec.effective_date):
            exprs.append(cast(ParamExpr, historify_spec.effective_date))

    for expr in exprs:
        field_name = expr.path[0]
        if field_name not in known:
            raise ETLCompilationError.from_issue(
                ETLErrorCode.UNKNOWN_PARAM_FIELD.issue(
                    component=step_type, field_name=field_name, params_type=params_type.__name__
                )
            )


def validate_file_path_templates(
    step_type: type[Any],
    params_type: type[Any],
    source_bindings: tuple[SourceBinding, ...],
    target_binding: TargetBinding,
) -> None:
    """Raise when a literal file path template references an undeclared field.

    Only non-alias paths are validated: alias URIs live in environment
    config (``storage.files``) and are resolved — and template-checked —
    at runtime.  As in :func:`validate_param_exprs`, public ``property``
    names on the params class count as known fields — template resolution
    uses the same ``getattr`` at runtime.
    """
    known = _known_fields(params_type)
    if known is None:
        return
    known = known | _class_properties(params_type)

    paths: list[str] = []
    for binding in source_bindings:
        spec = binding.spec
        if isinstance(spec, FileSourceSpec) and not spec.is_alias:
            paths.append(spec.path)
    target_spec = target_binding.spec
    if isinstance(target_spec, FileSpec) and not target_spec.is_alias:
        paths.append(target_spec.path)

    for path in paths:
        for field_name in extract_template_fields(path):
            if field_name not in known:
                raise ETLCompilationError.from_issue(
                    ETLErrorCode.UNKNOWN_TEMPLATE_FIELD.issue(
                        component=step_type,
                        field_name=field_name,
                        path=path,
                        params_type=params_type.__name__,
                    )
                )


def _known_fields(params_type: type[Any]) -> frozenset[str] | None:
    try:
        if not (isinstance(params_type, type) and issubclass(params_type, msgspec.Struct)):
            return None
        return frozenset(f.name for f in msgspec.structs.fields(params_type))
    except Exception:
        return None


def _class_properties(params_type: type[Any]) -> frozenset[str]:
    """Public ``property`` names declared on *params_type* (class inspection).

    Only properties count — methods and private names stay unknown.
    """
    return frozenset(
        name
        for name, member in inspect.getmembers(params_type)
        if isinstance(member, property) and not name.startswith("_")
    )


def _collect_exprs(node: PredicateNode, out: list[ParamExpr]) -> None:
    shape = _predicate_shape(node)
    if shape is _PredicateShape.BINARY:
        binary = cast(_BinaryPredicateLike, node)
        left = binary.left
        right = binary.right
        if _is_predicate_node_like(left) or _is_predicate_node_like(right):
            _collect_exprs(left, out)
            _collect_exprs(right, out)
            return
        _collect_value(left, out)
        _collect_value(right, out)
        return
    if shape is _PredicateShape.IN:
        in_pred = cast(_InPredicateLike, node)
        _collect_value(in_pred.ref, out)
        _collect_value(in_pred.values, out)
        return
    if shape is _PredicateShape.UNARY:
        unary = cast(_UnaryPredicateLike, node)
        _collect_exprs(unary.operand, out)


def _collect_value(value: Any, out: list[ParamExpr]) -> None:
    if _is_param_expr(value):
        out.append(value)


def _is_param_expr(value: Any) -> bool:
    if isinstance(value, ParamExpr):
        return True
    path = getattr(value, "path", None)
    return isinstance(path, tuple) and all(isinstance(part, str) for part in path)


def _predicate_shape(node: Any) -> _PredicateShape:
    if not isinstance(node, LoomFrozenStruct):
        return _PredicateShape.UNKNOWN
    fields = {field.name for field in msgspec.structs.fields(type(node))}
    if _has_fields(fields, _PredicateField.LEFT, _PredicateField.RIGHT):
        return _PredicateShape.BINARY
    if _has_fields(fields, _PredicateField.REF, _PredicateField.VALUES):
        return _PredicateShape.IN
    if _has_fields(fields, _PredicateField.OPERAND):
        return _PredicateShape.UNARY
    return _PredicateShape.UNKNOWN


def _is_predicate_node_like(value: Any) -> bool:
    return _predicate_shape(value) is not _PredicateShape.UNKNOWN


def _has_fields(fields: set[str], *expected: _PredicateField) -> bool:
    if len(fields) != len(expected):
        return False
    return all(field.value in fields for field in expected)


def _is_replace_where_like(spec: object) -> bool:
    return hasattr(spec, "replace_predicate") and hasattr(spec, "table_ref")


def _is_historify_like(spec: object) -> bool:
    return (
        isinstance(getattr(spec, "keys", None), tuple)
        and hasattr(spec, "effective_date")
        and hasattr(spec, "table_ref")
    )


def validate_upsert_spec(step_type: type[Any], spec: TargetSpec) -> None:
    """Validate UPSERT/UPDATE merge constraints at compile time.

    :class:`~loom.etl.declarative.target.UpdateSpec` mirrors the upsert merge fields, so the
    same checks apply — only the builder method named in the error changes.
    """
    if not _is_upsert_like(spec):
        return
    method = "update" if isinstance(spec, UpdateSpec) else "upsert"
    upsert_spec = cast(_UpsertSpecLike, spec)
    _check_upsert_keys_non_empty(step_type, upsert_spec, method)
    _check_upsert_exclude_include_exclusive(step_type, upsert_spec, method)
    _check_upsert_exclude_keys_disjoint(step_type, upsert_spec, method)
    _check_upsert_delete_flag(step_type, spec, method)
    if method == "update":
        _check_update_include_not_absorbed(step_type, upsert_spec)


def validate_historify_spec(step_type: type[Any], spec: TargetSpec) -> None:
    """Validate IntoHistory-specific constraints at compile time.

    Belt-and-suspenders check: :class:`~loom.etl.IntoHistory` already
    validates these constraints at construction time, but this validator
    runs again at compile time to catch any directly-constructed
    :class:`~loom.etl.HistorifySpec` instances that bypassed the builder.

    Args:
        step_type: The step class being compiled (used in error messages).
        spec:      Compiled target spec to inspect.
    """
    if not _is_historify_like(spec):
        return
    historify = cast(_HistorifySpecLike, spec)
    _check_historify_keys_non_empty(step_type, historify)
    _check_historify_keys_track_disjoint(step_type, historify)
    _check_historify_effective_date_not_in_track(step_type, historify)
    _check_historify_boundary_cols_disjoint(step_type, historify)
    _check_historify_log_mode_not_param_expr(step_type, historify)
    _check_historify_delete_marker(step_type, historify)
    _warn_historify_no_partition_scope(step_type, historify)
    _warn_historify_rerun_without_scope(step_type, historify)
    _warn_historify_track_none(step_type, historify)


def _check_historify_keys_non_empty(step_type: type[Any], spec: _HistorifySpecLike) -> None:
    if not spec.keys:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.HISTORIFY_KEYS_EMPTY.issue(component=step_type)
        )


def _check_historify_keys_track_disjoint(step_type: type[Any], spec: _HistorifySpecLike) -> None:
    if spec.track is None:
        return
    overlap = frozenset(spec.keys) & frozenset(spec.track)
    if overlap:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.HISTORIFY_KEYS_TRACK_OVERLAP.issue(
                component=step_type, overlap=str(sorted(overlap))
            )
        )


def _check_historify_effective_date_not_in_track(
    step_type: type[Any], spec: _HistorifySpecLike
) -> None:
    """Raise if effective_date column name appears in track."""
    if spec.track is None or not isinstance(spec.effective_date, str):
        return
    if spec.effective_date in spec.track:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.HISTORIFY_EFFECTIVE_DATE_IN_TRACK.issue(
                component=step_type, effective_date=spec.effective_date
            )
        )


def _check_historify_boundary_cols_disjoint(step_type: type[Any], spec: _HistorifySpecLike) -> None:
    """Raise if valid_from / valid_to / deleted_at clash with keys or track."""
    boundary = {spec.valid_from, spec.valid_to, spec.deleted_at}
    overlap_keys = boundary & frozenset(spec.keys)
    if overlap_keys:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.HISTORIFY_BOUNDARY_IN_KEYS.issue(
                component=step_type, overlap=str(sorted(overlap_keys))
            )
        )
    if spec.track is not None:
        overlap_track = boundary & frozenset(spec.track)
        if overlap_track:
            raise ETLCompilationError.from_issue(
                ETLErrorCode.HISTORIFY_BOUNDARY_IN_TRACK.issue(
                    component=step_type, overlap=str(sorted(overlap_track))
                )
            )


def _check_historify_log_mode_not_param_expr(
    step_type: type[Any], spec: _HistorifySpecLike
) -> None:
    """Raise if LOG mode uses a ParamExpr as effective_date (must be a column name)."""
    if str(spec.mode) != "log":
        return
    if _is_param_expr(spec.effective_date):
        raise ETLCompilationError.from_issue(
            ETLErrorCode.HISTORIFY_LOG_EFFECTIVE_DATE_NOT_COLUMN.issue(component=step_type)
        )


def _check_historify_delete_marker(step_type: type[Any], spec: _HistorifySpecLike) -> None:
    """Raise if delete_marker is set outside LOG mode or names a declared column."""
    marker = spec.delete_marker
    if marker is None:
        return
    if str(spec.mode) != "log":
        raise ETLCompilationError.from_issue(
            ETLErrorCode.HISTORIFY_DELETE_MARKER_REQUIRES_LOG.issue(component=step_type)
        )
    declared = {
        *spec.keys,
        *(spec.track or ()),
        *(spec.overwrite or ()),
        str(spec.effective_date),
        spec.valid_from,
        spec.valid_to,
        spec.deleted_at,
        *(spec.partition_scope or ()),
    }
    if marker in declared:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.HISTORIFY_DELETE_MARKER_DECLARED_COLUMN.issue(
                component=step_type, marker=marker
            )
        )


def _warn_historify_no_partition_scope(step_type: type[Any], spec: _HistorifySpecLike) -> None:
    if spec.partition_scope is None:
        _log.warning(
            "%s: IntoHistory declared without 'partition_scope' — "
            "each run will read and write the entire history table. "
            "Declare partition_scope to constrain Delta reads/writes to relevant partitions.",
            step_type.__qualname__,
        )


def _warn_historify_rerun_without_scope(step_type: type[Any], spec: _HistorifySpecLike) -> None:
    if spec.allow_temporal_rerun and spec.partition_scope is None:
        _log.warning(
            "%s: IntoHistory has allow_temporal_rerun=True but no 'partition_scope' — "
            "a re-weave will affect the entire history table. "
            "Declare partition_scope to limit re-weave scope.",
            step_type.__qualname__,
        )


def _warn_historify_track_none(step_type: type[Any], spec: _HistorifySpecLike) -> None:
    if spec.track is None:
        _log.warning(
            "%s: IntoHistory 'track=None' — tracked columns will be inferred "
            "at runtime from the incoming frame. "
            "Declare 'track' explicitly to fix the tracked columns at compile time.",
            step_type.__qualname__,
        )


def _check_upsert_keys_non_empty(step_type: type[Any], spec: _UpsertSpecLike, method: str) -> None:
    if not spec.upsert_keys:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.UPSERT_NO_KEYS.issue(component=step_type, method=method)
        )


def _check_upsert_exclude_include_exclusive(
    step_type: type[Any], spec: _UpsertSpecLike, method: str
) -> None:
    if spec.upsert_exclude and spec.upsert_include:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.UPSERT_EXCLUDE_INCLUDE_CONFLICT.issue(component=step_type, method=method)
        )


def _check_upsert_exclude_keys_disjoint(
    step_type: type[Any], spec: _UpsertSpecLike, method: str
) -> None:
    overlap = frozenset(spec.upsert_exclude) & frozenset(spec.upsert_keys)
    if overlap:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.UPSERT_KEY_IN_EXCLUDE.issue(
                component=step_type, overlap=str(sorted(overlap)), method=method
            )
        )


def _check_upsert_delete_flag(step_type: type[Any], spec: object, method: str) -> None:
    """Raise when ``delete_flag`` names a key, partition, exclude, or include column."""
    if not isinstance(spec, UpsertSpec):
        return
    conflict = upsert_delete_flag_conflict(spec)
    if conflict is None:
        return
    raise ETLCompilationError.from_issue(
        ETLErrorCode.UPSERT_DELETE_FLAG_CONFLICT.issue(
            component=step_type,
            method=method,
            delete_flag=spec.delete_flag,
            conflict=conflict,
        )
    )


def _check_update_include_not_absorbed(step_type: type[Any], spec: _UpsertSpecLike) -> None:
    """Raise when update() include= cannot produce a single UPDATE SET column.

    Keys and partition columns are always excluded from UPDATE SET, and
    update() never inserts — so an include list fully absorbed by them makes
    the whole write a guaranteed no-op: a green pipeline that repairs nothing.
    """
    if not spec.upsert_include:
        return
    always_excluded = frozenset(spec.upsert_keys) | frozenset(spec.partition_cols)
    if frozenset(spec.upsert_include) <= always_excluded:
        raise ETLCompilationError.from_issue(
            ETLErrorCode.UPDATE_EMPTY_UPDATE_SET.issue(
                component=step_type, include=str(sorted(spec.upsert_include))
            )
        )


def _is_upsert_like(spec: object) -> bool:
    keys = getattr(spec, "upsert_keys", None)
    exclude = getattr(spec, "upsert_exclude", None)
    include = getattr(spec, "upsert_include", None)
    return isinstance(keys, tuple) and isinstance(exclude, tuple) and isinstance(include, tuple)


__all__ = [
    "StepCompilationContext",
    "validate_execute_signature",
    "validate_historify_spec",
    "validate_param_exprs",
    "validate_params_compat",
    "validate_step",
    "validate_upsert_spec",
]
