"""Errors raised by the execution context."""

from __future__ import annotations

from loom.core.errors import SystemError


class ExecutionRequired(SystemError):
    """Raised when code reads the execution identity outside any execution.

    There is no fallback to the ambient identity: a handle called from a
    singleton constructor or an uncopied thread fails closed.
    """

    retryable = False


class ExecutionClosed(ExecutionRequired):
    """Raised when a task or thread reads a per-execution value of an execution that ended.

    Background work runs in an execution of its own: dispatch a job, or call
    ``executor.run(UseCase)`` from the task.
    """


class MarkerNotResolved(AttributeError):
    """Raised when an execution marker is used before its resolver replaced it.

    An :class:`AttributeError`: ``hasattr``, ``getattr`` with a default and the
    copy and pickle protocols read an unresolved marker as absent.

    Example::

        raise MarkerNotResolved("Mcp('acme-crm') was not resolved; is a resolver bound?")
    """
