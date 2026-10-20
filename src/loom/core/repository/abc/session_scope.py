"""Optional capability for repositories whose session belongs to the caller."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SupportsCallerScopedSession(Protocol):
    """Optional capability: report whether the caller owns the current session.

    A repository that resolves its session from the calling context — a
    ``@transactional`` scope, a unit of work — runs its reads inside a session
    that is closed when that caller unwinds, and whose uncommitted writes are
    visible only to it.  A wrapper that keeps or shares results must ask
    first: the repository cache takes no part while this returns ``True`` —
    no lookup, no store and no in-process coalescing — because such a read
    may see writes and settings only that caller has, and a coalesced load
    would outlive the caller that started it.

    A repository that does not implement this protocol is taken to own the
    scope of its own reads, which is what a repository opening and closing a
    session per call does.

    This capability is provisional: it exists because each backend publishes
    its transaction through its own ``ContextVar``, and it is not a stable
    extension point to build on.
    :func:`loom.core.transaction.in_atomic_transaction` does not supersede
    it: that marker answers whether *any* atomic transaction is open, not
    whether *this* repository's session belongs to the caller.
    """

    def has_caller_scoped_session(self) -> bool:
        """Whether the current context holds a session owned by the caller.

        Returns:
            ``True`` inside a caller-owned transaction, ``False`` when the
            repository would open and close a session of its own.
        """
        ...


@runtime_checkable
class SupportsCacheScope(Protocol):
    """Optional capability: the caller scope a wrapper keeps this repository's results in.

    What a read returns may depend on the caller, as row-level security makes
    it depend on the session's settings, so a wrapper that keeps results, such
    as the repository cache, keys them by this scope: a result kept for one
    scope is never served to another. It must tell apart at least the callers
    the repository's cursor tokens are bound apart for, since a kept first
    page carries such a token. A repository without it is kept under ``""``.

    Like :class:`SupportsCallerScopedSession`, this capability is provisional
    and is not a stable extension point to build on.
    """

    def cache_scope(self) -> str:
        """Return the caller's scope as cached results are kept by it; ``""`` when unscoped."""
        ...
