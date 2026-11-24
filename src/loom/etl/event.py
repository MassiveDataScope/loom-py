"""The names of what the ETL package logs: its events, their field keys and the schema drifts."""

from __future__ import annotations

from enum import StrEnum

from loom.core.log_names import LogEvent, LogKey, ValueKind


class EtlEvent(LogEvent):
    """The events the ETL compiler, executor, engines, sources and targets log."""

    CHECKPOINT_MISSED = "etl.checkpoint_missed"
    CHECKPOINT_HIT = "etl.checkpoint_hit"
    CHECKPOINT_WRITTEN = "etl.checkpoint_written"
    CHECKPOINT_CLEANED = "etl.checkpoint_cleaned"
    CHECKPOINT_CLEANUP_SKIPPED = "etl.checkpoint_cleanup_skipped"
    CHECKPOINT_OPTIONS_IGNORED = "etl.checkpoint_options_ignored"
    CHECKPOINT_ENCRYPTION_IGNORED = "etl.checkpoint_encryption_ignored"
    CHECKPOINT_RUN_CLEANUP_FAILED = "etl.checkpoint_run_cleanup_failed"
    CHECKPOINT_CORRELATION_CLEANUP_FAILED = "etl.checkpoint_correlation_cleanup_failed"
    CORRELATION_NOT_CLEANED = "etl.correlation_not_cleaned"

    PIPELINE_COMPILED = "etl.pipeline_compiled"
    STEP_COMPILED = "etl.step_compiled"
    COMPILE_CACHE_HIT = "etl.compile_cache_hit"
    APPEND_TARGET_COMPILED = "etl.append_target_compiled"
    HISTORIFY_SCOPE_MISSING = "etl.historify_scope_missing"
    HISTORIFY_RERUN_UNSCOPED = "etl.historify_rerun_unscoped"
    HISTORIFY_TRACK_INFERRED = "etl.historify_track_inferred"

    YAML_LOADED = "etl.yaml_loaded"
    PLAN_FILTERED = "etl.plan_filtered"
    PIPELINE_STARTED = "etl.pipeline_started"
    PROCESS_STARTED = "etl.process_started"
    STEP_STARTED = "etl.step_started"
    SOURCE_READ_STARTED = "etl.source_read_started"
    TEMP_SOURCE_READ_STARTED = "etl.temp_source_read_started"
    TARGET_WRITE_STARTED = "etl.target_write_started"
    TEMP_TARGET_WRITE_STARTED = "etl.temp_target_write_started"

    MERGE_PARTITIONS_UNDECLARED = "etl.merge_partitions_undeclared"
    MERGE_PARTITIONS_FOUND = "etl.merge_partitions_found"
    HISTORIFY_RERUN_REQUIRED = "etl.historify_rerun_required"
    WRITE_PRODUCED_NO_ROWS = "etl.write_produced_no_rows"
    DELTA_WRITE_STARTED = "etl.delta_write_started"
    DELTA_WRITE_COMMITTED = "etl.delta_write_committed"
    REPLACE_PARTITIONS_EMPTY = "etl.replace_partitions_empty"
    CREATE_SKIPPED_EMPTY = "etl.create_skipped_empty"
    UNITY_CATALOG_CREATE_UNREGISTERED = "etl.unity_catalog_create_unregistered"
    PINNED_ROWS_RELEASE_FAILED = "etl.pinned_rows_release_failed"
    FILE_STREAM_WRITE_STARTED = "etl.file_stream_write_started"

    SOURCE_QUERY_STARTED = "etl.source_query_started"
    SOURCE_READ_COMPLETED = "etl.source_read_completed"
    SOURCE_SCHEMA_INFERRED = "etl.source_schema_inferred"
    SOURCE_BATCH_READ = "etl.source_batch_read"
    SOURCE_READ_DEGRADED = "etl.source_read_degraded"
    SOURCE_VALUE_UNSERIALISABLE = "etl.source_value_unserialisable"
    SOURCE_TYPE_STRINGIFIED = "etl.source_type_stringified"
    SOURCE_ROW_FLAGGED = "etl.source_row_flagged"
    SOURCE_SUBFIELDS_DROPPED = "etl.source_subfields_dropped"
    SOURCE_CAST_SKIPPED = "etl.source_cast_skipped"
    SOURCE_SCHEMA_DRIFTED = "etl.source_schema_drifted"
    SOURCE_STRING_FIELDS_SERIALISED = "etl.source_string_fields_serialised"
    SOURCE_UNDECLARED_FIELDS_SERIALISED = "etl.source_undeclared_fields_serialised"
    SOURCE_CONFLICT_DEFERRED = "etl.source_conflict_deferred"
    SOURCE_FRAME_BUILD_FELL_BACK = "etl.source_frame_build_fell_back"
    DYNAMODB_SEGMENT_FAILED = "etl.dynamodb_segment_failed"
    BSON_TYPE_UNKNOWN = "etl.bson_type_unknown"
    BSON_MAPPING_UNKNOWN = "etl.bson_mapping_unknown"
    BSON_MAPPING_FAILED = "etl.bson_mapping_failed"

    CLICKHOUSE_TABLE_TRUNCATED = "etl.clickhouse_table_truncated"
    CLICKHOUSE_INSERT_STARTED = "etl.clickhouse_insert_started"

    MAINTENANCE_NOT_CONFIGURED = "etl.maintenance_not_configured"
    MAINTENANCE_TABLE_SKIPPED = "etl.maintenance_table_skipped"
    MAINTENANCE_FAILED = "etl.maintenance_failed"
    TABLE_VACUUMED = "etl.table_vacuumed"
    TABLE_COMPACTED = "etl.table_compacted"
    TABLE_Z_ORDERED = "etl.table_z_ordered"


class EtlKey(LogKey):
    """The fields of an ETL record."""

    PIPELINE = "loom.etl.pipeline"
    PROCESS = "loom.etl.process"
    STEP = "loom.etl.step"
    RUN_ID = "loom.etl.run_id"
    CHECKPOINT = "loom.etl.checkpoint"
    SCOPE = "loom.etl.scope"
    OPTION_KEYS = "loom.etl.option_keys", ValueKind.ARRAY

    TABLE = "loom.etl.table"
    URI = "loom.etl.uri"
    MODE = "loom.etl.mode"
    KIND = "loom.etl.kind"
    VERSION = "loom.etl.version", ValueKind.NUMBER
    PREDICATES = "loom.etl.predicates", ValueKind.NUMBER
    COLUMNS = "loom.etl.columns", ValueKind.ARRAY
    RERUN_DATES = "loom.etl.rerun_dates", ValueKind.ARRAY

    ROWS = "loom.etl.rows", ValueKind.NUMBER
    BATCHES = "loom.etl.batches", ValueKind.NUMBER
    FILES = "loom.etl.files", ValueKind.NUMBER
    PARTITIONS = "loom.etl.partitions", ValueKind.NUMBER
    SERIALIZATION_FAILURES = "loom.etl.serialization_failures", ValueKind.NUMBER
    SEGMENT = "loom.etl.segment", ValueKind.NUMBER

    VALUE_TYPE = "loom.etl.value_type"
    ISSUES = "loom.etl.issues", ValueKind.ARRAY
    SHAPE = "loom.etl.shape", ValueKind.ARRAY
    DRIFT = "loom.etl.drift"


class SchemaDrift(StrEnum):
    """How a source batch differed from its schema: the value of :attr:`EtlKey.DRIFT`."""

    FIELDS_APPEARED = "fields_appeared"
    FIELDS_ABSENT = "fields_absent"
    FIELDS_DROPPED = "fields_dropped"
    UNDECLARED_FIELDS_DROPPED = "undeclared_fields_dropped"
    TYPES_CONFLICTED = "types_conflicted"
    NESTED_TYPES_CONFLICTED = "nested_types_conflicted"
