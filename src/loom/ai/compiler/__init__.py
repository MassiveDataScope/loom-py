"""Agent compiler: offline, multi-error compilation of authored artifacts.

Turns a decoded :class:`~loom.ai.declarative.AgentSpecV1` into an immutable
:class:`AgentPlan` — the only artifact-derived input every downstream stage
reads.  Compilation is fully offline: no network, no credentials, no
entry-point loading, and every problem found across every phase (and every
spec, in :meth:`AgentCompiler.compile_all`) is reported at once through a
single :class:`AgentCompilationError` with stable
:class:`~loom.ai.errors.AgentErrorCode` values.

Example:
    >>> compiler = AgentCompiler(
    ...     config=ai_config, registry=registry, supported_kinds=kinds
    ... )
    >>> plan = compiler.compile(spec, source_path="agents/triage.agent.yaml")
"""

from loom.ai.compiler._compiler import AgentCompiler
from loom.ai.compiler._plan import (
    AgentPlan,
    CompiledA2ACapability,
    CompiledCapability,
    CompiledConversation,
    CompiledInstruction,
    CompiledMcpCapability,
    CompiledNativeCapability,
    CompiledOutput,
    CompiledOutputHook,
    CompiledPythonCapability,
    CompiledRemoteAuth,
    CompiledSkillsCapability,
    CompiledSqlCapability,
    CompiledUsecaseCapability,
    mcp_connection,
)
from loom.ai.errors import AgentCompilationError, AgentCompilationIssue, AgentErrorCode

__all__ = [
    "AgentCompilationError",
    "AgentCompilationIssue",
    "AgentCompiler",
    "AgentErrorCode",
    "AgentPlan",
    "CompiledA2ACapability",
    "CompiledNativeCapability",
    "CompiledCapability",
    "CompiledConversation",
    "CompiledInstruction",
    "CompiledMcpCapability",
    "CompiledOutput",
    "CompiledOutputHook",
    "CompiledPythonCapability",
    "CompiledRemoteAuth",
    "CompiledSkillsCapability",
    "CompiledSqlCapability",
    "CompiledUsecaseCapability",
    "mcp_connection",
]
