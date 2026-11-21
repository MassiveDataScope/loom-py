"""The field keys of the streaming runtime."""

from __future__ import annotations

from loom.core.logger import LogKey


class StreamingLogField(LogKey):
    """The field keys of a flow, its nodes and the messages they carry."""

    FLOW = "loom.streaming.flow"
    FLOW_RUN_ID = "loom.flow_run_id"
    NODE_INDEX = "loom.streaming.node_index"
    NODE_TYPE = "loom.streaming.node_type"
    SINK = "loom.streaming.sink"
    MESSAGE_ID = "loom.message_id"
    PARENT_TRACE_ID = "loom.parent_trace_id"
    CAUSATION_ID = "loom.causation_id"
    MESSAGE_TYPE = "loom.message_type"
    KAFKA_PARTITION = "messaging.kafka.partition"
    KAFKA_OFFSET = "messaging.kafka.offset"
    BATCH_ID = "loom.batch_id"
    BATCH_SIZE = "loom.batch_size"
    LINKS_TRUNCATED = "loom.links_truncated"
    TERMINAL_REASON = "loom.terminal.reason"
    FAILURE_SCOPE = "loom.terminal.failure_scope"
    DLQ_TOPIC = "loom.terminal.dlq_topic"
    ERROR_KIND = "loom.streaming.error_kind"
    DECLARED_ROUTES = "loom.declared_routes"
    HAS_DEFAULT_ROUTE = "loom.has_default_route"
