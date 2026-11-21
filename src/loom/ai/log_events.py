"""The field keys of the AI package."""

from __future__ import annotations

from loom.core.logger import LogKey


class AiLogField(LogKey):
    """The field keys only the AI package writes: OpenTelemetry GenAI names where defined."""

    AGENT = "gen_ai.agent.name"
    CAPABILITY = "loom.ai.capability"
    MCP_SERVER = "loom.ai.mcp_server"
    INTERACTION_ID = "loom.ai.interaction_id"
    OUTPUT_CORRECTION = "loom.ai.output_correction"
    USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
    USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
    USAGE_CACHE_READ_INPUT_TOKENS = "gen_ai.usage.cache_read.input_tokens"
    USAGE_CACHE_CREATION_INPUT_TOKENS = "gen_ai.usage.cache_creation.input_tokens"
    USAGE_REQUESTS = "gen_ai.usage.requests"
    USAGE_TOOL_CALLS = "gen_ai.usage.tool_calls"
    USAGE_COST = "gen_ai.usage.cost"
    USAGE_COST_KNOWN = "gen_ai.usage.cost_known"
