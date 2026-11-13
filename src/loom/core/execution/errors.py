"""Errors raised by the execution context."""

from __future__ import annotations

from loom.core.errors import SystemError


class ExecutionRequired(SystemError):
    """Raised when code reads the execution identity outside any execution.

    The identity of a call is the identity of the execution in progress, read
    when the call happens. Outside an execution there is no such identity, and
    this error is raised instead of falling back to the ambient
    :func:`~loom.core.identity.current_identity`: a handle used from a
    singleton constructor, a module import or a thread pool that did not copy
    the context fails closed rather than acting as whoever set the identity
    last.

    Args:
        message: Names what needed the execution.

    Example::

        raise ExecutionRequired("acme.billing.InvoiceAgent was called outside an execution")
    """

    retryable = False  # a programming error: a retry meets it again


class MarkerNotResolved(AttributeError):
    """Raised when an execution marker is used before its resolver replaced it.

    A subclass of :class:`AttributeError` on purpose: ``hasattr``, ``getattr``
    with a default and the copy and pickle protocols probe attributes and
    expect that exact exception, so an unresolved marker answers them as
    "absent" instead of breaking them.

    Example::

        raise MarkerNotResolved("Mcp('acme-crm') was not resolved; is a resolver bound?")
    """
