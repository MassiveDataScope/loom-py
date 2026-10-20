"""Optional capability for repositories whose session belongs to the caller."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SupportsCallerScopedSession(Protocol):
    """Optional capability: report whether the caller owns the current session.

    A repository that resolves its session from the calling context — a
    ``@transactional`` scope, a unit of work — runs its reads inside a session
    that is closed when that caller unwinds, and whose uncommitted writes are
    visible only to it.  A wrapper that would otherwise detach a read from its
    caller must ask first: the cache layer skips its in-process coalescing
    while this returns ``True``, because a coalesced load outlives the caller
    that started it and would be shared with another one.

    A repository that does not implement this protocol is taken to own the
    scope of its own reads, which is what a repository opening and closing a
    session per call does.

    This capability is provisional: it exists because each backend publishes
    its transaction through its own ``ContextVar``.  It is expected to be
    replaced by the neutral transaction scope, and is not a stable extension
    point to build on.
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
