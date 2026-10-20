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
class SupportsCursorScope(Protocol):
    """Optional capability: the caller scope this repository binds cursor tokens to.

    A token is valid only in the scope it was issued in, so a wrapper that
    keeps pages, such as the query cache, keys them by this scope too. A
    repository without it binds its tokens to no scope.

    Like :class:`SupportsCallerScopedSession`, this capability is provisional
    and is not a stable extension point to build on.
    """

    def cursor_scope(self) -> str:
        """Return the caller's scope as cursor tokens bind it; ``""`` when unscoped."""
        ...
