"""The log events of the engine."""

from __future__ import annotations

from loom.core.logger import LogEvent


class CompilerLogEvent(LogEvent, prefix="compiler"):
    """What the compiler logs."""

    USE_CASE_COMPILED = "compiler.use_case_compiled"
