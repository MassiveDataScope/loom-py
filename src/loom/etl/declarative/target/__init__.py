"""ETL target declaration API — builders and compiled spec types.

Builders (user-facing):

* :class:`IntoTable`   — Delta table target
* :class:`IntoFile`    — file target (CSV, JSON, XLSX, Parquet)
* :class:`IntoTemp`    — intermediate store target
* :class:`IntoHistory` — SCD Type 2 history target
* :class:`SchemaMode`  — schema evolution strategy

Spec types (internal — compiler, executor, and backend writers):

TABLE targets (Delta Lake):

* :class:`AppendSpec`             — append rows
* :class:`ReplaceSpec`            — full table overwrite
* :class:`ReplacePartitionsSpec`  — overwrite partitions present in the frame
* :class:`ReplaceWhereSpec`       — overwrite rows matching a static predicate
* :class:`UpsertSpec`             — MERGE with explicit merge keys
* :class:`UpdateSpec`             — matched-only MERGE (never inserts)
* :class:`HistorifySpec`          — SCD Type 2 merge with history tracking

Non-table targets:

* :class:`FileSpec`       — write to a file (CSV / JSON / Parquet / XLSX)
* :class:`TempSpec`       — write to intermediate store (single writer)
* :class:`TempFanInSpec`  — write to intermediate store (fan-in; multiple writers)

SCD2 support types:

* :class:`HistorifyInputMode`          — snapshot vs log input semantics
* :class:`DeletePolicy`                — action for absent keys in snapshot mode
* :class:`HistoryDateType`             — date vs timestamp boundary columns
* :class:`HistorifyRepairReport`       — re-weave operation report
* :class:`HistorifyKeyConflictError`   — duplicate entity state vectors
* :class:`HistorifyDateCollisionError` — same-date LOG mode collisions
* :class:`HistorifyDeleteMarkerError`  — LOG mode delete marker missing or not boolean
* :class:`HistorifyTemporalConflictError` — future-open records without re-weave

Write errors:

* :class:`EmptyReplaceError` — ``replace(allow_empty=False)`` received no rows
* :class:`UpsertDeleteFlagError` — ``upsert(delete_flag=...)`` column unusable
* :class:`UnsupportedUpsertDeleteFlagError` — writer cannot apply ``delete_flag``
"""

from loom.etl.declarative.target._client import ClientSpec, IntoClient
from loom.etl.declarative.target._errors import (
    EmptyReplaceError,
    UnsupportedUpsertDeleteFlagError,
    UpsertDeleteFlagError,
)
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyDateCollisionError,
    HistorifyDeleteMarkerError,
    HistorifyInputMode,
    HistorifyKeyConflictError,
    HistorifyRepairReport,
    HistorifySpec,
    HistorifyTemporalConflictError,
    HistoryDateType,
    IntoHistory,
)
from loom.etl.declarative.target._into import IntoFile, IntoTable, IntoTemp
from loom.etl.declarative.target._schema_mode import SchemaMode
from loom.etl.declarative.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpdateSpec,
    UpsertSpec,
)
from loom.etl.declarative.target._temp import TempFanInSpec, TempSpec

TargetSpec = (
    AppendSpec
    | ReplaceSpec
    | ReplacePartitionsSpec
    | ReplaceWhereSpec
    | UpsertSpec
    | UpdateSpec
    | HistorifySpec
    | FileSpec
    | TempSpec
    | TempFanInSpec
    | ClientSpec
)
"""Union of all concrete target spec variants.

Used as the public type annotation for any value produced by ``_to_spec``
on any target builder (:class:`IntoTable`, :class:`IntoFile`,
:class:`IntoTemp`, :class:`IntoHistory`, :class:`IntoClient`).

.. note::
    :class:`ClientSpec` is included in this union because it is produced by
    :meth:`IntoClient._to_spec`, but it is **never** passed to
    :meth:`~loom.etl.runtime.contracts.TargetWriter.write`.  The executor
    intercepts it before the write path and routes it to
    :class:`~loom.etl.runtime.contracts.ClientCommandExecutor`.  Both
    :class:`~loom.etl.backends._write_policy.WritePolicy` and
    :class:`~loom.etl.io.targets.ClickHouseTargetWriter` reject it explicitly
    with a :exc:`TypeError` as a defensive guard.
"""

__all__ = [
    # builders
    "IntoTable",
    "IntoFile",
    "IntoTemp",
    "IntoHistory",
    "IntoClient",
    # schema
    "SchemaMode",
    # union
    "TargetSpec",
    # client sentinel
    "ClientSpec",
    # table specs
    "AppendSpec",
    "ReplaceSpec",
    "ReplacePartitionsSpec",
    "ReplaceWhereSpec",
    "UpsertSpec",
    "UpdateSpec",
    "HistorifySpec",
    # file / temp specs
    "FileSpec",
    "TempSpec",
    "TempFanInSpec",
    # SCD2 enums
    "HistorifyInputMode",
    "DeletePolicy",
    "HistoryDateType",
    # SCD2 report
    "HistorifyRepairReport",
    # SCD2 errors
    "HistorifyKeyConflictError",
    "HistorifyDateCollisionError",
    "HistorifyDeleteMarkerError",
    "HistorifyTemporalConflictError",
    # write errors
    "EmptyReplaceError",
    "UpsertDeleteFlagError",
    "UnsupportedUpsertDeleteFlagError",
]
