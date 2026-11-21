"""MongoDB batch processor — list[dict] → pl.DataFrame."""

from __future__ import annotations

import datetime
import json
import logging
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

import polars as pl

from loom.core.errors.boundary import boundary
from loom.etl.declarative.source._specs import ExtraFieldsMode
from loom.etl.io.sources._mongo_bson import _is_bson_type, deep_normalize_for_json
from loom.etl.io.sources._serialization import _record_serialization_failure

_log = logging.getLogger(__name__)

_LIST_CLASSIFY_SAMPLE = 20
_MAX_NESTED_DEPTH = 64
_MAX_RISKY_ROWS_REPORTED = 5
_MAX_RISKY_NOTES_PER_ROW = 8
_SHAPE_SUMMARY_DEPTH = 8
_SHAPE_SUMMARY_LIMIT = 24


def _json_default(obj: object) -> object:
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.hex()
    if _is_bson_type(obj):
        return deep_normalize_for_json(obj)
    _log.warning(
        "MongoSourceReader: unexpected type %s in document field — using str()",
        type(obj).__name__,
    )
    return str(obj)


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, default=_json_default, ensure_ascii=False)


def _safe_dumps(v: Any) -> str | None:
    if v is None:
        return None
    try:
        return _json_dumps(v)
    except Exception as exc:  # noqa: BLE001
        _record_serialization_failure(exc, v)
        return None


def _classify_value(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, bytes):
        return "bytes"
    if isinstance(v, (datetime.datetime, datetime.date)):
        return "datetime"
    if isinstance(v, dict):
        return "dict"
    if isinstance(v, list):
        return "null" if not v else _classify_list(v)
    return f"other:{type(v).__name__}"


def _classify_list(v: list[Any]) -> str:
    seen: set[str] = set()
    count = 0
    for item in v:
        if item is None:
            continue
        seen.add(_classify_value(item))
        count += 1
        if len(seen) > 1 or count >= _LIST_CLASSIFY_SAMPLE:
            break
    if not seen:
        return "list"
    return f"list[{next(iter(seen))}]" if len(seen) == 1 else "list[mixed]"


def _effective_type_set(ts: set[str]) -> set[str]:
    base = ts - {"int", "float"}
    return base | ({"numeric"} if ts & {"int", "float"} else set())


def _record_value_tags(
    tags: dict[str, set[str]],
    is_complex: dict[str, bool],
    root_key: str,
    sub_path: str,
    value: Any,
    depth: int,
) -> None:
    if depth >= _MAX_NESTED_DEPTH:
        tags.setdefault(sub_path, set()).add("depth_cap")
        return
    if value is None:
        return
    if isinstance(value, dict):
        is_complex[root_key] = True
        tags.setdefault(sub_path, set()).add("dict")
        for sk, sv in value.items():
            _record_value_tags(tags, is_complex, root_key, f"{sub_path}.{sk}", sv, depth + 1)
        return
    if isinstance(value, list):
        is_complex[root_key] = is_complex.get(root_key, False) or any(
            isinstance(item, dict) for item in value
        )
        tags.setdefault(sub_path, set()).add("list")
        for item in value:
            _record_value_tags(tags, is_complex, root_key, f"{sub_path}[]", item, depth + 1)
        return
    tags.setdefault(sub_path, set()).add(_classify_value(value))


def _classify_batch_keys(
    batch: list[dict[str, Any]],
) -> tuple[set[str], set[str]]:
    """Return (conflicted_keys, complex_root_keys) in a single pass over the batch.

    Records per-path type tags directly instead of materialising every complex
    value into intermediate lists; memory is O(distinct sub-paths) per batch.
    """
    root_types: dict[str, set[str]] = {}
    sub_tags: dict[str, dict[str, set[str]]] = {}
    is_complex: dict[str, bool] = {}
    for doc in batch:
        for k, v in doc.items():
            t = _classify_value(v)
            if t != "null":
                root_types.setdefault(k, set()).add(t)
            if isinstance(v, (dict, list)):
                inner = sub_tags.setdefault(k, {})
                _record_value_tags(inner, is_complex, k, "$", v, 0)
    conflicted = {k for k, ts in root_types.items() if len(_effective_type_set(ts)) > 1}
    complex_root = {
        k
        for k, paths in sub_tags.items()
        if is_complex.get(k, False)
        and any(len(_effective_type_set(ts)) > 1 for ts in paths.values())
    }
    return conflicted, complex_root


def _pre_serialize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        return v
    try:
        return _json_dumps(deep_normalize_for_json(v))
    except Exception as exc:  # noqa: BLE001
        _record_serialization_failure(exc, v)
        return None


def _preserialize_str_fields(
    doc: dict[str, Any],
    str_fields: frozenset[str] | set[str],
    str_complex: set[str],
) -> None:
    for k in str_fields:
        if k not in doc:
            continue
        v = doc[k]
        if isinstance(v, (dict, list)):
            str_complex.add(k)
        doc[k] = _pre_serialize_value(v)


def _classify_doc_fields(
    doc: dict[str, Any],
    declared: set[str],
    root_types: dict[str, set[str]],
    sub_tags: dict[str, dict[str, set[str]]],
    is_complex: dict[str, bool],
    undeclared_complex: set[str],
) -> None:
    for k, v in doc.items():
        t = _classify_value(v)
        if t != "null":
            root_types.setdefault(k, set()).add(t)
        if isinstance(v, (dict, list)):
            if declared and k not in declared:
                undeclared_complex.add(k)
            _record_value_tags(sub_tags.setdefault(k, {}), is_complex, k, "$", v, 0)


def _serialize_conflicted(batch: list[dict[str, Any]], conflicted: set[str]) -> None:
    if not conflicted:
        return
    for doc in batch:
        for k in conflicted:
            if k in doc and doc[k] is not None:
                doc[k] = _safe_dumps(doc[k])


def _series_to_json_string(s: pl.Series) -> pl.Series:
    if s.dtype == pl.String:
        return s
    if isinstance(s.dtype, (pl.Struct, pl.List, pl.Array)):
        return s.map_elements(boundary(_safe_dumps), return_dtype=pl.String)
    return s.cast(pl.String)


def _summarize_nested_shapes(
    value: Any, path: str = "$", depth: int = 0, limit: int = _SHAPE_SUMMARY_LIMIT
) -> list[str]:
    if depth >= _SHAPE_SUMMARY_DEPTH or limit <= 0:
        return []
    lines: list[str] = []
    if isinstance(value, dict):
        lines.append(f"{path}: dict keys={list(value.keys())}")
        remaining = limit - 1
        for key, item in value.items():
            if remaining <= 0:
                break
            nested = _summarize_nested_shapes(item, f"{path}.{key}", depth + 1, remaining)
            lines.extend(nested)
            remaining = limit - len(lines)
        return lines
    if isinstance(value, list):
        lines.append(f"{path}: list len={len(value)}")
        remaining = limit - 1
        for idx, item in enumerate(value[: min(len(value), remaining)]):
            nested = _summarize_nested_shapes(item, f"{path}[{idx}]", depth + 1, remaining)
            lines.extend(nested)
            remaining = limit - len(lines)
            if remaining <= 0:
                break
        return lines
    lines.append(f"{path}: {type(value).__name__}")
    return lines


def _row_id(doc: dict[str, Any]) -> Any:
    for key in ("_id", "id", "event_id", "root_id"):
        if key in doc:
            return doc[key]
    return None


def _scalar_risk_notes(value: Any, plan: _CanonicalValuePlan, path: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (dict, list)):
        return [f"{path}: expected scalar got {type(value).__name__}"]
    if isinstance(value, str):
        coerced = _coerce_string_to_dtype(value, plan.dtype)
        if isinstance(coerced, str) and plan.dtype not in (None, pl.String):
            return [f"{path}: unconvertible str for {plan.dtype}"]
    return []


def _list_risk_notes(value: Any, plan: _CanonicalValuePlan, path: str) -> list[str]:
    if not isinstance(value, list):
        return [f"{path}: expected list got {type(value).__name__}"]
    if plan.inner is None:
        return []
    notes: list[str] = []
    for idx, item in enumerate(value[:5]):
        notes.extend(_value_risk_notes(item, plan.inner, f"{path}[{idx}]"))
    return notes


def _struct_risk_notes(value: Any, plan: _CanonicalValuePlan, path: str) -> list[str]:
    if not isinstance(value, dict):
        return [f"{path}: expected struct got {type(value).__name__}"]
    expected = {field.name for field in plan.fields}
    notes: list[str] = []
    extra = [key for key in value if key not in expected]
    if extra:
        notes.append(f"{path}: extra key(s) {sorted(extra)!r}")
    for field in plan.fields:
        if field.name in value:
            notes.extend(_value_risk_notes(value[field.name], field.plan, f"{path}.{field.name}"))
    return notes


def _value_risk_notes(value: Any, plan: _CanonicalValuePlan, path: str) -> list[str]:
    if value is None:
        return []
    if plan.kind == "scalar":
        return _scalar_risk_notes(value, plan, path)
    if plan.kind == "list":
        return _list_risk_notes(value, plan, path)
    if plan.kind == "struct":
        return _struct_risk_notes(value, plan, path)
    return []


def _row_risk_notes(
    doc: dict[str, Any], compiled_plan: dict[str, _CanonicalValuePlan]
) -> list[str]:
    notes: list[str] = []
    declared = set(compiled_plan)
    for key, value in doc.items():
        if key not in declared:
            if isinstance(value, (dict, list)):
                notes.append(f"$.{key}: undeclared complex {type(value).__name__}")
            continue
        notes.extend(_value_risk_notes(value, compiled_plan[key], f"$.{key}"))
    return notes


def _log_risky_rows(
    batch: list[dict[str, Any]],
    schema_overrides: dict[str, pl.DataType] | None,
    source_label: str = "MongoSourceReader",
) -> None:
    if not batch or not schema_overrides:
        return
    compiled_plan = _build_schema_plan(schema_overrides)
    reported = 0
    for row_index, doc in enumerate(batch):
        notes = _row_risk_notes(doc, compiled_plan)
        if not notes:
            continue
        reported += 1
        _log.warning(
            "%s: risky row sample row=%d id=%r issues=%s",
            source_label,
            row_index,
            _row_id(doc),
            notes[:_MAX_RISKY_NOTES_PER_ROW],
        )
        if reported >= _MAX_RISKY_ROWS_REPORTED:
            break


@dataclass(frozen=True, slots=True)
class _CanonicalFieldPlan:
    name: str
    plan: _CanonicalValuePlan


@dataclass(frozen=True, slots=True)
class _CanonicalValuePlan:
    kind: str
    dtype: pl.DataType | None = None
    fields: tuple[_CanonicalFieldPlan, ...] = ()
    inner: _CanonicalValuePlan | None = None


def _plan_from_dtype(dtype: pl.DataType) -> _CanonicalValuePlan:
    if isinstance(dtype, pl.Struct):
        return _CanonicalValuePlan(
            kind="struct",
            fields=tuple(
                _CanonicalFieldPlan(name=field.name, plan=_plan_from_dtype(field.dtype))  # type: ignore[arg-type]
                for field in dtype.fields
            ),
        )
    if isinstance(dtype, (pl.List, pl.Array)):
        return _CanonicalValuePlan(kind="list", inner=_plan_from_dtype(dtype.inner))  # type: ignore[arg-type]
    return _CanonicalValuePlan(kind="scalar", dtype=dtype)


_INT_DTYPES = frozenset(
    {pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64}
)
_FLOAT_DTYPES = frozenset({pl.Float32, pl.Float64})
_BOOL_TRUTHY = frozenset({"true", "t", "1", "yes", "y"})
_BOOL_FALSY = frozenset({"false", "f", "0", "no", "n"})


def _coerce_int_string(text: str, original: str) -> Any:
    try:
        return int(text)
    except ValueError:
        return original


def _coerce_float_string(text: str, original: str) -> Any:
    try:
        return float(text)
    except ValueError:
        return original


def _coerce_bool_string(text: str, original: str) -> Any:
    lowered = text.lower()
    if lowered in _BOOL_TRUTHY:
        return True
    if lowered in _BOOL_FALSY:
        return False
    return original


def _coerce_temporal_string(text: str, original: str, dtype: pl.DataType) -> Any:
    if isinstance(dtype, pl.Datetime):
        try:
            return datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return original
    if isinstance(dtype, pl.Date):
        try:
            return datetime.date.fromisoformat(text)
        except ValueError:
            return original
    if isinstance(dtype, pl.Time):
        try:
            return datetime.time.fromisoformat(text)
        except ValueError:
            return original
    return original


def _coerce_string_to_dtype(value: Any, dtype: pl.DataType | None) -> Any:
    """Coerce a str value to the target Polars dtype. Non-str values are returned unchanged."""
    if value is None or dtype is None:
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return None
    if dtype == pl.String:
        return value
    if dtype in _INT_DTYPES:
        return _coerce_int_string(text, value)
    if dtype in _FLOAT_DTYPES:
        return _coerce_float_string(text, value)
    if dtype == pl.Boolean:
        return _coerce_bool_string(text, value)
    if isinstance(dtype, (pl.Datetime, pl.Date, pl.Time)):
        return _coerce_temporal_string(text, value, dtype)
    return value


def _build_schema_plan(schema_overrides: dict[str, pl.DataType]) -> dict[str, _CanonicalValuePlan]:
    return {name: _plan_from_dtype(dtype) for name, dtype in schema_overrides.items()}


def _declared_complex_fields(schema_overrides: dict[str, pl.DataType] | None) -> frozenset[str]:
    """Return the names of fields explicitly declared as Struct, List, or Array.

    These fields are excluded from conflict-serialisation so that the schema
    canonicalisation step can coerce their sub-values instead of flattening the
    whole field to a JSON string.
    """
    if not schema_overrides:
        return frozenset()
    return frozenset(
        name
        for name, dtype in schema_overrides.items()
        if isinstance(dtype, (pl.Struct, pl.List, pl.Array))
    )


def _coerce_list_items(value: list[Any], inner: _CanonicalValuePlan) -> list[Any]:
    return [_canonicalize_value(item, inner) for item in value]


def _coerce_struct_fields(
    value: dict[str, Any], fields: tuple[_CanonicalFieldPlan, ...]
) -> dict[str, Any]:
    declared = {f.name for f in fields}
    extra = sorted(k for k in value if k not in declared)
    if extra:
        _log.warning(
            "MongoSourceReader: dropping undeclared sub-field(s) %s from declared struct",
            extra,
        )
    return {f.name: _canonicalize_value(value.get(f.name), f.plan) for f in fields}


def _canonicalize_value(value: Any, plan: _CanonicalValuePlan) -> Any:
    if plan.kind == "scalar":
        return _coerce_string_to_dtype(value, plan.dtype)
    if value is None:
        return None
    if plan.kind == "list":
        if not isinstance(value, list):
            return None
        return value if plan.inner is None else _coerce_list_items(value, plan.inner)
    if plan.kind == "struct":
        return _coerce_struct_fields(value, plan.fields) if isinstance(value, dict) else None
    return value


def _canonicalize_batch(
    batch: list[dict[str, Any]],
    plan: dict[str, _CanonicalValuePlan],
) -> None:
    if not batch or not plan:
        return
    for doc in batch:
        for field_name, field_plan in plan.items():
            if field_name in doc:
                doc[field_name] = _canonicalize_value(doc[field_name], field_plan)


def _apply_series_override(
    s: pl.Series, k: str, values: list[Any], override: pl.DataType
) -> pl.Series:
    if s.dtype == override:
        return s
    if override == pl.String and isinstance(s.dtype, (pl.Struct, pl.List, pl.Array)):
        return pl.Series(name=k, values=[_safe_dumps(v) for v in values], dtype=pl.String)
    try:
        return s.cast(override, strict=False)
    except Exception as exc:
        _log.debug(
            "MongoSourceReader: could not cast %r to %s — leaving inferred type (%s)",
            k,
            override,
            type(exc).__name__,
        )
        return s


def _build_frame_fallback(
    batch: list[dict[str, Any]],
    schema_overrides: dict[str, pl.DataType] | None = None,
) -> pl.DataFrame:
    if not batch:
        return pl.DataFrame()
    all_keys = list(dict.fromkeys(k for doc in batch for k in doc))
    cols = {k: [doc.get(k) for doc in batch] for k in all_keys}
    series_list = []
    for k in all_keys:
        try:
            s = pl.Series(name=k, values=cols[k])
        except (pl.exceptions.ComputeError, pl.exceptions.SchemaError, TypeError):
            s = pl.Series(name=k, values=[_safe_dumps(v) for v in cols[k]], dtype=pl.String)
        override = (schema_overrides or {}).get(k)
        if override is not None:
            s = _apply_series_override(s, k, cols[k], override)
        series_list.append(s)
    return pl.DataFrame(series_list)


def align_to_schema(
    df: pl.DataFrame,
    schema: dict[str, pl.DataType],
    source_label: str = "MongoSourceReader",
) -> pl.DataFrame:
    """Align a schemaless-path batch to the sample-inferred registered schema.

    Args:
        df:           Batch frame to align.
        schema:       Registered schema (column → dtype).
        source_label: Reader name used as prefix in emitted log messages.
    """
    if not schema:
        return df
    exprs: list[pl.Expr | pl.Series] = []
    for col_name, target_dtype in schema.items():
        if col_name not in df.columns:
            exprs.append(pl.lit(None, dtype=target_dtype).alias(col_name))
            continue
        col_dtype = df[col_name].dtype
        if col_dtype == target_dtype:
            continue
        if target_dtype == pl.String and isinstance(col_dtype, (pl.Struct, pl.List, pl.Array)):
            exprs.append(_series_to_json_string(df[col_name]).alias(col_name))
        elif col_dtype != pl.String:
            exprs.append(pl.col(col_name).cast(target_dtype, strict=False).alias(col_name))
    if exprs:
        df = df.with_columns(exprs)
    new_cols = [c for c in df.columns if c not in schema]
    if new_cols:
        _log.warning(
            "%s: schema drift — field(s) %s not in registered schema, dropping",
            source_label,
            sorted(new_cols),
        )
        df = df.drop(new_cols)
    return df.select(list(schema.keys()))


def apply_declared_schema(
    df: pl.DataFrame,
    declared: dict[str, pl.DataType],
    mode: ExtraFieldsMode,
    schema_name: str,
    source_label: str = "MongoSourceReader",
) -> pl.DataFrame:
    """Add missing declared columns and dispatch extra columns per extra_fields_mode.

    Args:
        df:           Batch frame to reshape.
        declared:     User-declared schema (column → dtype).
        mode:         How undeclared fields are handled.
        schema_name:  Collection or table name used in messages.
        source_label: Reader name used as prefix in emitted messages.

    Raises:
        ValueError: When *mode* is :attr:`ExtraFieldsMode.ERROR` and undeclared fields are present.
    """
    missing = [
        pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in declared.items()
        if name not in df.columns
    ]
    if missing:
        df = df.with_columns(missing)

    extra_cols = [c for c in df.columns if c not in declared]
    if extra_cols:
        if mode == ExtraFieldsMode.ERROR:
            raise ValueError(
                f"{source_label}: documents contain fields not declared in "
                f"{schema_name}: {extra_cols}. "
                "Use on_extra_fields('ignore'), ('warn'), or ('capture') to suppress."
            )
        if mode == ExtraFieldsMode.WARN:
            _log.warning(
                "%s: dropping %d undeclared field(s) from %s: %s",
                source_label,
                len(extra_cols),
                schema_name,
                extra_cols,
            )
        if mode == ExtraFieldsMode.CAPTURE:
            extra_struct = pl.struct([df[c] for c in extra_cols])
            extra_series = extra_struct.map_elements(
                boundary(_safe_dumps), return_dtype=pl.String
            ).alias("_extra")
            df = df.drop(extra_cols).with_columns(extra_series)
        else:
            df = df.drop(extra_cols)

    if mode == ExtraFieldsMode.CAPTURE and "_extra" not in df.columns:
        df = df.with_columns(pl.lit(None, pl.String()).alias("_extra"))

    output_cols = list(declared.keys()) + (["_extra"] if mode == ExtraFieldsMode.CAPTURE else [])
    return df if df.columns == output_cols else df.select(output_cols)


class MongoBatchProcessor:
    """Transform a list[dict] batch into a pl.DataFrame.

    Handles pre-serialization of declared/explicit JSON fields, type-conflict
    detection and serialization, and DataFrame construction with schema hints.
    Schema alignment and extra-field handling are delegated to the caller.

    Not thread-safe: designed for single-threaded use inside a Polars scan plugin.

    Args:
        schema_str_fields: Field names explicitly declared as String in the user schema.
            These are pre-serialised to JSON before conflict detection.
        declared_schema: The schema explicitly declared by the user (not inferred).
            Used only for risky-row logging to avoid false positives on inferred types.
        source_label: Reader name used as prefix in emitted log messages.
    """

    def __init__(
        self,
        *,
        schema_str_fields: frozenset[str],
        declared_schema: dict[str, pl.DataType] | None = None,
        source_label: str = "MongoSourceReader",
    ) -> None:
        self._schema_str_fields = schema_str_fields
        self._declared_schema = declared_schema
        self._source_label = source_label
        self._str_coercion_warned = False
        self._canonical_plan: dict[str, _CanonicalValuePlan] | None = None
        self._observed_fields: frozenset[str] | None = None

    def _check_schema_drift(self, batch: list[dict[str, Any]]) -> None:
        """Warn when fields appear or disappear relative to the first batch seen."""
        current_fields = frozenset(k for doc in batch for k in doc)
        if self._observed_fields is None:
            self._observed_fields = current_fields
            return
        new_fields = current_fields - self._observed_fields
        dropped_fields = self._observed_fields - current_fields
        if new_fields:
            _log.warning(
                "%s: schema drift — new field(s) %s appeared after first batch",
                self._source_label,
                sorted(new_fields),
            )
            self._observed_fields = self._observed_fields | new_fields
        if dropped_fields:
            _log.warning(
                "%s: schema drift — field(s) %s absent in this batch",
                self._source_label,
                sorted(dropped_fields),
            )

    def build_frame(
        self,
        batch: list[dict[str, Any]],
        schema_overrides: dict[str, pl.DataType] | None = None,
    ) -> pl.DataFrame:
        if not batch:
            return pl.DataFrame()
        self._check_schema_drift(batch)
        str_complex, root_types, sub_tags, is_complex, undeclared_complex = self._scan_batch(
            batch, schema_overrides
        )
        self._warn_str_coercion_once(str_complex)
        to_serialize = self._resolve_conflict_fields(
            root_types, sub_tags, is_complex, undeclared_complex, schema_overrides
        )
        if to_serialize:
            _serialize_conflicted(batch, to_serialize)
        if schema_overrides:
            if self._canonical_plan is None:
                self._canonical_plan = _build_schema_plan(schema_overrides)
            _canonicalize_batch(batch, self._canonical_plan)
        _log_risky_rows(batch, self._declared_schema, self._source_label)
        return self._to_dataframe(batch, schema_overrides)

    def _scan_batch(
        self,
        batch: list[dict[str, Any]],
        schema_overrides: dict[str, pl.DataType] | None,
    ) -> tuple[
        set[str], dict[str, set[str]], dict[str, dict[str, set[str]]], dict[str, bool], set[str]
    ]:
        declared = set(schema_overrides) if schema_overrides else set()
        str_fields = self._schema_str_fields
        str_complex: set[str] = set()
        root_types: dict[str, set[str]] = {}
        sub_tags: dict[str, dict[str, set[str]]] = {}
        is_complex: dict[str, bool] = {}
        undeclared_complex: set[str] = set()
        for doc in batch:
            _preserialize_str_fields(doc, str_fields, str_complex)
            _classify_doc_fields(
                doc, declared, root_types, sub_tags, is_complex, undeclared_complex
            )
        return str_complex, root_types, sub_tags, is_complex, undeclared_complex

    def _warn_str_coercion_once(self, str_complex: set[str]) -> None:
        if self._str_coercion_warned or not str_complex:
            return
        _log.debug(
            "%s: field(s) %s declared as str in schema but data contains "
            "complex types — pre-serialising to JSON string",
            self._source_label,
            sorted(str_complex),
        )
        self._str_coercion_warned = True

    def _resolve_conflict_fields(
        self,
        root_types: dict[str, set[str]],
        sub_tags: dict[str, dict[str, set[str]]],
        is_complex: dict[str, bool],
        undeclared_complex: set[str],
        schema_overrides: dict[str, pl.DataType] | None,
    ) -> set[str]:
        protected = _declared_complex_fields(schema_overrides)
        all_conflicted = {k for k, ts in root_types.items() if len(_effective_type_set(ts)) > 1}
        all_nested = {
            k
            for k, paths in sub_tags.items()
            if is_complex.get(k, False)
            and any(len(_effective_type_set(ts)) > 1 for ts in paths.values())
        } - all_conflicted

        to_serialize = (all_conflicted | all_nested | undeclared_complex) - protected
        skipped = (all_conflicted | all_nested) & protected

        if to_serialize & all_conflicted:
            _log.warning(
                "%s: heterogeneous types detected in fields %s — serialising to JSON string",
                self._source_label,
                sorted(to_serialize & all_conflicted),
            )
        if to_serialize & all_nested:
            _log.warning(
                "%s: nested type conflict — serialising parent field(s) %s to JSON string",
                self._source_label,
                sorted(to_serialize & all_nested),
            )
        extras_only = (to_serialize & undeclared_complex) - all_conflicted - all_nested
        if extras_only:
            _log.debug(
                "%s: serialising undeclared complex field(s) %s to JSON string",
                self._source_label,
                sorted(extras_only),
            )
        if skipped:
            _log.warning(
                "%s: type conflict in declared complex field(s) %s"
                " — deferring to schema canonicalization",
                self._source_label,
                sorted(skipped),
            )
        return to_serialize

    def _to_dataframe(
        self, batch: list[dict[str, Any]], schema_overrides: dict[str, pl.DataType] | None
    ) -> pl.DataFrame:
        try:
            return pl.from_dicts(
                batch,
                schema_overrides=schema_overrides or None,
                strict=False,
                infer_schema_length=None,
            )
        except (pl.exceptions.ComputeError, pl.exceptions.SchemaError, TypeError):
            if batch:
                _log.warning(
                    "%s: pl.from_dicts() failed — "
                    "falling back to series construction. Batch shape sample:\n%s",
                    self._source_label,
                    "\n".join(_summarize_nested_shapes(batch[0])),
                )
            return _build_frame_fallback(batch, schema_overrides)


@runtime_checkable
class BatchProcessorProtocol(Protocol):
    """Structural interface for batch processors used by MongoSourceReader.

    Any object implementing ``build_frame`` is compatible, enabling injection of
    alternative processors (e.g., mocks, instrumented versions) without subclassing.

    Args:
        batch: Raw normalised documents (output of ``normalize_bson_doc``).
        schema_overrides: Polars dtype hints, either declared or inferred.

    Returns:
        A ``pl.DataFrame`` with one row per document.
    """

    def build_frame(
        self,
        batch: list[dict[str, Any]],
        schema_overrides: dict[str, pl.DataType] | None = None,
    ) -> pl.DataFrame: ...


__all__ = [
    "BatchProcessorProtocol",
    "MongoBatchProcessor",
    "align_to_schema",
    "apply_declared_schema",
]
