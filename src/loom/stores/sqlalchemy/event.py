"""What the relational store logs and the fields of each record."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey, ValueKind


class SqlalchemyEvent(LogEvent):
    """The events the relational store logs."""

    SESSION_MANAGER_INITIALIZED = "loom.sqlalchemy.session_manager_initialized"
    SESSION_MANAGER_CLOSED = "loom.sqlalchemy.session_manager_closed"
    SESSION_OPENED = "loom.sqlalchemy.session_opened"
    SESSION_CLOSED = "loom.sqlalchemy.session_closed"
    OPERATION_SESSION_ROLLED_BACK = "loom.sqlalchemy.operation_session_rolled_back"
    POOL_SIZING_OMITTED = "loom.sqlalchemy.pool_sizing_omitted"
    STORES_NOT_LOADED = "loom.sqlalchemy.stores_not_loaded"
    TRANSACTION_SESSION_REUSED = "loom.sqlalchemy.transaction_session_reused"
    TRANSACTION_COMMITTED = "loom.sqlalchemy.transaction_committed"
    TRANSACTION_ROLLED_BACK = "loom.sqlalchemy.transaction_rolled_back"
    REPOSITORY_TRANSACTION_COMMITTED = "loom.sqlalchemy.repository_transaction_committed"
    NO_MODELS_DISCOVERED = "loom.sqlalchemy.no_models_discovered"
    READINESS_PROBE_FAILED = "loom.sqlalchemy.readiness_probe_failed"
    SCOPED_TABLES_UNPROTECTED = "loom.sqlalchemy.scoped_tables_unprotected"
    AUDIT_PARTITION_MISSING = "loom.sqlalchemy.audit_partition_missing"
    AUDIT_PARTITIONS_NEARLY_EXHAUSTED = "loom.sqlalchemy.audit_partitions_nearly_exhausted"


class SqlalchemyKey(LogKey):
    """The fields of a relational store record."""

    BACKEND = "loom.sqlalchemy.backend"
    DRIVER = "loom.sqlalchemy.driver"
    INJECT_TRACE_ID = "loom.sqlalchemy.inject_trace_id", ValueKind.BOOLEAN
    HAS_SESSION_SETTINGS = "loom.sqlalchemy.has_session_settings", ValueKind.BOOLEAN
    SKIPPED_STORES = "loom.sqlalchemy.skipped_stores", ValueKind.ARRAY
    OWNER = "loom.sqlalchemy.owner"
    METHOD = "loom.sqlalchemy.method"
    MUTATION_COUNT = "loom.sqlalchemy.mutation_count", ValueKind.NUMBER
    TABLES = "loom.sqlalchemy.tables", ValueKind.ARRAY
    MONTHS_COVERED = "loom.sqlalchemy.months_covered", ValueKind.NUMBER
