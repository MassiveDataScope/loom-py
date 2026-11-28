"""What the AI runtime logs and the fields of each record."""

from __future__ import annotations

from enum import StrEnum

from loom.core.log_names import LogEvent, LogKey, ValueKind


class AiEvent(LogEvent):
    """The events the AI runtime logs."""

    REMOTE_PLAINTEXT_ACCEPTED = "loom.ai.remote_plaintext_accepted"
    REMOTE_CLIENT_SKIPPED = "loom.ai.remote_client_skipped"
    PRICE_MODEL_UNUSED = "loom.ai.price_model_unused"
    MODEL_UNPRICEABLE = "loom.ai.model_unpriceable"
    ENGINE_NATIVE_SUPPORT_MISSING = "loom.ai.engine_native_support_missing"
    A2A_AGENT_NOT_COMPILED = "loom.ai.a2a_agent_not_compiled"
    A2A_AGENT_PUBLISHED = "loom.ai.a2a_agent_published"
    ENDPOINT_AGENT_NOT_COMPILED = "loom.ai.endpoint_agent_not_compiled"
    ENDPOINT_MOUNTED = "loom.ai.endpoint_mounted"
    ENDPOINT_FAILED = "loom.ai.endpoint_failed"
    RUN_FAILED = "loom.ai.run_failed"
    RUN_FAILED_FROM_CAUSE = "loom.ai.run_failed_from_cause"
    ROW_FAILED = "loom.ai.row_failed"
    STREAM_FAILED = "loom.ai.stream_failed"
    STEP_FAILED = "loom.ai.step_failed"
    STEP_CANCELLED_AT_BOUND = "loom.ai.step_cancelled_at_bound"
    STEP_FAILED_AFTER_CONSUMER_LEFT = "loom.ai.step_failed_after_consumer_left"
    STEP_COMPLETED_AFTER_CONSUMER_LEFT = "loom.ai.step_completed_after_consumer_left"
    HEALTH_PROBE_FAILED = "loom.ai.health_probe_failed"
    HISTORY_INVALID = "loom.ai.history_invalid"
    HISTORY_TOO_LARGE = "loom.ai.history_too_large"
    TOOL_FAILED = "loom.ai.tool_failed"
    TOOL_REMOTE_AGENT_UNREACHABLE = "loom.ai.tool_remote_agent_unreachable"
    MCP_SESSION_CLOSE_FAILED = "loom.ai.mcp_session_close_failed"
    MCP_POOL_CLOSE_FAILED = "loom.ai.mcp_pool_close_failed"
    MCP_SESSION_RENEWED = "loom.ai.mcp_session_renewed"
    CANCELLED_CALL_DRAINED = "loom.ai.cancelled_call_drained"


class AiKey(LogKey):
    """The fields of an AI record."""

    AGENT = "loom.ai.agent"
    CONFIG_PATH = "loom.ai.config_path"
    ENGINE = "loom.ai.engine"
    MODEL = "loom.ai.model"
    TOOL = "loom.ai.tool"
    STEP = "loom.ai.step"
    INTERACTION = "loom.ai.interaction"
    PLAINTEXT_HOST = "loom.ai.plaintext_host"
    ANONYMOUS = "loom.ai.anonymous", ValueKind.BOOLEAN
    CONVERSATIONAL = "loom.ai.conversational", ValueKind.BOOLEAN


class PlaintextHost(StrEnum):
    """The host a plaintext URL was let through to: the value of :attr:`AiKey.PLAINTEXT_HOST`."""

    LOOPBACK = "loopback"
    PRIVATE = "private"
