"""The log events and field keys of the bootstrap."""

from __future__ import annotations

from loom.core.logger import LogEvent, LogKey


class BootstrapLogEvent(LogEvent):
    """What the bootstrap logs while it composes the container and the kernel."""

    CONTAINER_INITIALISING = "bootstrap.container_initialising"
    MODULES_REGISTERED = "bootstrap.modules_registered"
    USE_CASES_COMPILING = "bootstrap.use_cases_compiling"
    CONTAINER_VALIDATING = "bootstrap.container_validating"
    KERNEL_COMPLETED = "bootstrap.kernel_completed"


class BootstrapLogField(LogKey):
    """The field keys only the bootstrap writes."""

    MODULES = "loom.bootstrap.modules"
