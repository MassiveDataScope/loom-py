"""What the AI runtime logs and the fields of each record."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey


class AiEvent(LogEvent):
    """The events the AI runtime logs."""

    REMOTE_PLAINTEXT_LOOPBACK = "ai.remote_plaintext_loopback"
    REMOTE_PLAINTEXT_PRIVATE = "ai.remote_plaintext_private"
    REMOTE_CLIENT_SKIPPED = "ai.remote_client_skipped"
    PRICE_MODEL_UNUSED = "ai.price_model_unused"
    MODEL_UNPRICEABLE = "ai.model_unpriceable"
    ENGINE_NATIVE_SUPPORT_MISSING = "ai.engine_native_support_missing"
    A2A_AGENT_NOT_COMPILED = "ai.a2a_agent_not_compiled"
    A2A_AGENT_PUBLISHED = "ai.a2a_agent_published"
    A2A_AGENT_PUBLISHED_ANONYMOUS = "ai.a2a_agent_published_anonymous"
    A2A_AGENT_PUBLISHED_ANONYMOUS_CONVERSATIONAL = "ai.a2a_agent_published_anonymous_conversational"
    ENDPOINT_AGENT_NOT_COMPILED = "ai.endpoint_agent_not_compiled"
    ENDPOINT_MOUNTED = "ai.endpoint_mounted"
    ENDPOINT_MOUNTED_ANONYMOUS = "ai.endpoint_mounted_anonymous"
    ENDPOINT_MOUNTED_ANONYMOUS_CONVERSATIONAL = "ai.endpoint_mounted_anonymous_conversational"
    ENDPOINT_FAILED = "ai.endpoint_failed"
    RUN_FAILED = "ai.run_failed"
    RUN_FAILED_FROM_CAUSE = "ai.run_failed_from_cause"
    ROW_FAILED = "ai.row_failed"
    STREAM_FAILED = "ai.stream_failed"
    STEP_FAILED = "ai.step_failed"
    STEP_CANCELLED_AT_BOUND = "ai.step_cancelled_at_bound"
    STEP_FAILED_AFTER_CONSUMER_LEFT = "ai.step_failed_after_consumer_left"
    STEP_COMPLETED_AFTER_CONSUMER_LEFT = "ai.step_completed_after_consumer_left"
    HEALTH_PROBE_FAILED = "ai.health_probe_failed"
    HISTORY_INVALID = "ai.history_invalid"
    HISTORY_TOO_LARGE = "ai.history_too_large"
    TOOL_FAILED = "ai.tool_failed"
    TOOL_REMOTE_AGENT_UNREACHABLE = "ai.tool_remote_agent_unreachable"
    MCP_SESSION_CLOSE_FAILED = "ai.mcp_session_close_failed"
    MCP_POOL_CLOSE_FAILED = "ai.mcp_pool_close_failed"
    MCP_SESSION_RENEWED = "ai.mcp_session_renewed"
    CANCELLED_CALL_DRAINED = "ai.cancelled_call_drained"


class AiKey(LogKey):
    """The fields of an AI record."""

    AGENT = "loom.ai.agent"
    CONFIG_PATH = "loom.ai.config_path"
    ENGINE = "loom.ai.engine"
    ROUTE = "loom.ai.route"
    MODEL = "loom.ai.model"
    TOOL = "loom.ai.tool"
    STEP = "loom.ai.step"
    INTERACTION = "loom.ai.interaction"
