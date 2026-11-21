"""The log events and field keys of the engine."""

from __future__ import annotations

from loom.core.logger import LogEvent, LogKey


class EngineLogEvent(LogEvent):
    """What the engine logs: each use case it compiles, and the steps of an execution."""

    USE_CASE_COMPILED = "compiler.use_case_compiled"
    EXECUTION_STEPS = "engine.execution_steps"


class EngineLogField(LogKey):
    """The field keys only the engine writes."""

    STEPS = "loom.engine.steps"
