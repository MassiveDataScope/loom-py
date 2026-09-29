"""Who holds which role where, and where those assignments come from."""

from __future__ import annotations

import functools
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from loom.core.authz._roles import require_name
from loom.core.authz._scope import Scope


def require_aware(name: str, moment: datetime) -> None:
    """Reject a moment that does not name a single instant.

    Args:
        name: What the moment is, for the error message.
        moment: The moment to check.

    Raises:
        TypeError: When *moment* is not a datetime.
        ValueError: When *moment* is naive.
    """
    if not isinstance(moment, datetime):
        raise TypeError(f"{name} must be a datetime, not {type(moment).__name__}.")
    if moment.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware: {moment!r}.")


def _require_subject(subject: str) -> None:
    if not subject:
        raise ValueError("A grant needs a subject.")


def _require_scope(scope: object) -> None:
    if not isinstance(scope, Scope):
        raise TypeError(f"A grant scope must be a Scope, not {type(scope).__name__}.")


def _utc_or_none(name: str, moment: datetime | None) -> datetime | None:
    if moment is None:
        return None
    require_aware(name, moment)
    return moment.astimezone(UTC)


_NEVER = (0,)


@functools.total_ordering
@dataclass(frozen=True, slots=True)
class Grant:
    """An assignment of a role to a subject on a scope, possibly until a moment.

    Grants order by subject, role, scope and then expiry, with grants that
    never expire before those that do, soonest first.

    Attributes:
        subject: Opaque identifier of the holder, such as an identity subject
            or a service account id.
        role: Name of the role; its permissions are looked up in the catalog
            at decision time.
        scope: Where the role applies, including everything below it.
        expires_at: The timezone-aware instant from which the grant allows
            nothing; ``None`` when it never expires.
    """

    subject: str
    role: str
    scope: Scope
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        """Reject a grant with no holder, an unusable role name, no scope or a naive expiry."""
        _require_subject(self.subject)
        require_name("role", self.role)
        _require_scope(self.scope)
        object.__setattr__(self, "expires_at", _utc_or_none("A grant expiry", self.expires_at))

    @property
    def _sort_key(self) -> tuple[str, str, Scope, tuple[int] | tuple[int, datetime]]:
        expiry = _NEVER if self.expires_at is None else (1, self.expires_at)
        return (self.subject, self.role, self.scope, expiry)

    def __lt__(self, other: object) -> bool:
        """Order by subject, role, scope, then expiry."""
        if not isinstance(other, Grant):
            return NotImplemented
        return self._sort_key < other._sort_key


class GrantSource(Protocol):
    """Where the grants of a subject are loaded from.

    Any class with this method satisfies the protocol structurally, with no
    inheritance required.
    """

    async def grants_for(self, subject: str) -> Collection[Grant]:
        """Return every grant held by *subject*.

        Args:
            subject: Identifier of the holder.

        Returns:
            The subject's grants; empty when it holds none.
        """
        ...


class InMemoryGrantSource:
    """A :class:`GrantSource` over a fixed list, for tests and examples."""

    __slots__ = ("_grants",)

    def __init__(self, grants: Iterable[Grant]) -> None:
        """Hold *grants*, deduplicated in their original order.

        Args:
            grants: Grants of any number of subjects.
        """
        self._grants = tuple(dict.fromkeys(grants))

    async def grants_for(self, subject: str) -> Collection[Grant]:  # NOSONAR S7503 - protocol
        """Return the grants of *subject*, in the order they were given.

        Args:
            subject: Identifier of the holder.

        Returns:
            The subject's grants.
        """
        return tuple(grant for grant in self._grants if grant.subject == subject)
