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
    """Raised when a task or thread reads the execution it copied after that execution ended.

    A task created during an execution copies its context. Once the
    execution ends its identity and per-execution values are gone, so the
    task reads no caller rather than the one that has left. Background work
    runs in an execution of its own: dispatch a job from inside the
    execution, or call ``executor.run(UseCase, identity=...)`` from the task,
    which opens its own execution.
    """


class MarkerNotResolved(AttributeError):
    """Raised when an execution marker is used before its resolver replaced it.

    A subclass of :class:`AttributeError` on purpose: ``hasattr``, ``getattr``
    with a default and the copy and pickle protocols probe attributes and
    expect that exact exception, so an unresolved marker answers them as
    "absent" instead of breaking them.

    Example::

        raise MarkerNotResolved("Mcp('acme-crm') was not resolved; is a resolver bound?")
    """
