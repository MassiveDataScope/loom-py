"""What the streaming runtime logs and the fields of each record."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey, ValueKind


class StreamingEvent(LogEvent):
    """The events the streaming runtime logs."""

    SINK_FLUSHED = "streaming.sink_flushed"
    SINK_FLUSH_FAILED = "streaming.sink_flush_failed"
    DELTA_STAGING_LIMIT_REACHED = "streaming.delta_staging_limit_reached"
    DELTA_STAGED_RECORDS_DISCARDED = "streaming.delta_staged_records_discarded"
    STORAGE_SINK_COUNT_INVALID = "streaming.storage_sink_count_invalid"
    STORAGE_SINK_CLOSED_PENDING = "streaming.storage_sink_closed_pending"
    RECORD_PROBED = "streaming.record_probed"
    RECORD_UNDECODABLE = "streaming.record_undecodable"
    RECORD_TYPE_UNKNOWN = "streaming.record_type_unknown"
    OFFSET_COMMIT_FAILED = "streaming.offset_commit_failed"
    TOPIC_NOT_VISIBLE = "streaming.topic_not_visible"
    RESUME_STATE_BEHIND = "streaming.resume_state_behind"
    UNROUTED_ERRORS_DROPPED = "streaming.unrouted_errors_dropped"
    BOUNDARY_ERROR_MANAGED = "streaming.boundary_error_managed"
    BOUNDARY_ERROR_UNHANDLED = "streaming.boundary_error_unhandled"
    RUNNER_SHUT_DOWN = "streaming.runner_shut_down"
    MONGO_CDC_EVENT_SKIPPED = "streaming.mongo_cdc_event_skipped"
    MONGO_CDC_MESSAGE_SKIPPED = "streaming.mongo_cdc_message_skipped"
    MONGO_CDC_OPLOG_EXPIRED = "streaming.mongo_cdc_oplog_expired"


class StreamingKey(LogKey):
    """The fields of a streaming record."""

    FLOW = "loom.streaming.flow"
    STEP = "loom.streaming.step"
    NODE_INDEX = "loom.streaming.node_index", ValueKind.NUMBER
    NODE_TYPE = "loom.streaming.node_type"
    KIND = "loom.streaming.kind"
    TARGET = "loom.streaming.target"
    RECORDS = "loom.streaming.records", ValueKind.NUMBER
    TOPIC = "loom.streaming.topic"
    PARTITION = "loom.streaming.partition", ValueKind.NUMBER
    PARTITIONS = "loom.streaming.partitions", ValueKind.ARRAY
    OFFSET = "loom.streaming.offset", ValueKind.NUMBER
    MESSAGE_TYPE = "loom.streaming.message_type"
    REPLAYED_OFFSETS = "loom.streaming.replayed_offsets", ValueKind.NUMBER
