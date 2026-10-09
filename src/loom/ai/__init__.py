"""Loom AI pillar.

Package root for the AI agent layer. It stays import-light on purpose: no
optional engine extra may be imported here, so ``import loom.ai`` succeeds on a
base installation, and engine types are never re-exported.

The programmatic contracts exported here — these classes, protocols and
functions — are experimental: they may change within a major line (FR-056).
The artifact format is not experimental: an agent definition declaring
``spec_version: 1`` keeps validating and compiling for the whole major line
(FR-056a).  Author against the artifact; pin the Python API.
"""

from __future__ import annotations

from loom.ai.abc import (
    AgentAnswer,
    AgentEngine,
    AgentEngineProvider,
    AgentEvent,
    AgentHandle,
    AgentResult,
    AgentUsage,
    Attachment,
    ConcurrentMcpSession,
    Conversation,
    DepsFactory,
    ErrorEvent,
    FinalEvent,
    HealthState,
    HealthStatus,
    McpHandle,
    McpSession,
    McpToolCallResult,
    McpToolInfo,
    NativeCapableEngine,
    OutputCheck,
    Prompt,
    SqlGrantHandle,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
    ToolsetContext,
    ToolsetFactory,
)
from loom.ai.config import A2AConfig, AgentEndpointConfig, AiConfig
from loom.ai.errors import (
    AgentCompilationError,
    AgentCompilationIssue,
    AgentErrorCode,
    AgentRunError,
    AgentRunErrorClass,
    AgentRunErrorCode,
    is_retriable,
)
from loom.ai.inference import InferenceTarget
from loom.ai.pricing import ModelPrice
from loom.ai.runtime import AgentHealth, AgentRuntime

__all__ = [
    "AgentCompilationError",
    "AgentCompilationIssue",
    "AgentErrorCode",
    "AgentRunErrorClass",
    "AgentRunErrorCode",
    "HealthState",
    "is_retriable",
    "A2AConfig",
    "AgentAnswer",
    "AgentEndpointConfig",
    "AgentEngine",
    "AgentEngineProvider",
    "AgentEvent",
    "AgentHandle",
    "AgentHealth",
    "AgentResult",
    "AgentRunError",
    "AgentRuntime",
    "AgentUsage",
    "AiConfig",
    "Attachment",
    "ConcurrentMcpSession",
    "Conversation",
    "DepsFactory",
    "ErrorEvent",
    "FinalEvent",
    "HealthStatus",
    "InferenceTarget",
    "McpHandle",
    "McpSession",
    "McpToolCallResult",
    "McpToolInfo",
    "ModelPrice",
    "NativeCapableEngine",
    "OutputCheck",
    "Prompt",
    "SqlGrantHandle",
    "TextDeltaEvent",
    "ToolCallEvent",
    "ToolResultEvent",
    "ToolsetContext",
    "ToolsetFactory",
]
