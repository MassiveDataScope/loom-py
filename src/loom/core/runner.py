"""Runner contracts shared by Loom domains.

The shared contract stays deliberately small. Runner shapes differ by
domain, so this module only exposes optional lifecycle capabilities that
can be checked structurally via ``isinstance``.
"""

from __future__ import annotations

import logging
from typing import Protocol, runtime_checkable

from loom.core.log_names import LogEvent, LogKey

_log = logging.getLogger(__name__)


class RunnerEvent(LogEvent):
    """What the runner helpers log."""

    SHUTDOWN_FAILED = "loom.runner.shutdown_failed"
    FLUSH_FAILED = "loom.runner.flush_failed"


class RunnerKey(LogKey):
    """The fields of a runner record."""

    TYPE = "loom.runner.type"


@runtime_checkable
class SupportsShutdown(Protocol):
    """Structural protocol for runners that own shutdownable resources."""

    def shutdown(self) -> None:
        """Release any runner-owned resources."""
        ...


@runtime_checkable
class SupportsFlush(Protocol):
    """Structural protocol for runners that can flush buffered state."""

    def flush(self) -> None:
        """Flush buffered metrics, events, or other deferred state."""
        ...


def shutdown_runner(runner: object) -> None:
    """Call ``shutdown()`` on *runner* when it implements :class:`SupportsShutdown`.

    Designed for unconditional use in ``finally`` blocks: when ``shutdown()``
    raises, the exception is logged at ``WARNING`` level and suppressed so
    that the original exception propagating through the ``finally`` is
    preserved.  A no-op when *runner* does not satisfy the protocol.

    Args:
        runner: Any object. Capability is tested structurally via
            :class:`SupportsShutdown`; no base class is required.
    """
    if not isinstance(runner, SupportsShutdown):
        return
    try:
        runner.shutdown()
    except Exception:
        _log.warning(
            RunnerEvent.SHUTDOWN_FAILED,
            extra={RunnerKey.TYPE: type(runner).__name__},
            exc_info=True,
        )


def flush_runner(runner: object) -> None:
    """Call ``flush()`` on *runner* when it implements :class:`SupportsFlush`.

    Designed for unconditional use in ``finally`` blocks: when ``flush()``
    raises, the exception is logged at ``WARNING`` level and suppressed so
    that the original exception propagating through the ``finally`` is
    preserved.  A no-op when *runner* does not satisfy the protocol.

    Args:
        runner: Any object. Capability is tested structurally via
            :class:`SupportsFlush`; no base class is required.
    """
    if not isinstance(runner, SupportsFlush):
        return
    try:
        runner.flush()
    except Exception:
        _log.warning(
            RunnerEvent.FLUSH_FAILED,
            extra={RunnerKey.TYPE: type(runner).__name__},
            exc_info=True,
        )


__all__ = [
    "SupportsFlush",
    "SupportsShutdown",
    "flush_runner",
    "shutdown_runner",
]
