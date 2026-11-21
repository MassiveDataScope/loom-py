"""The log events of the bootstrap."""

from __future__ import annotations

from loom.core.logger import LogEvent


class BootstrapLogEvent(LogEvent, prefix="bootstrap"):
    """What the bootstrap logs while it composes the container and the kernel."""

    CONTAINER_INITIALISING = "bootstrap.container_initialising"
    MODULE_REGISTERING = "bootstrap.module_registering"
    USE_CASES_COMPILING = "bootstrap.use_cases_compiling"
    CONTAINER_VALIDATING = "bootstrap.container_validating"
    KERNEL_COMPLETED = "bootstrap.kernel_completed"
