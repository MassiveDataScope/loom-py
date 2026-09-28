"""Who holds which role where, and where those assignments come from."""

from __future__ import annotations

from collections.abc import Collection, Iterable
from dataclasses import dataclass
from typing import Protocol

from loom.core.authz._roles import require_name
from loom.core.authz._scope import Scope


@dataclass(frozen=True, slots=True, order=True)
class Grant:
    """An assignment of a role to a subject on a scope.

    Attributes:
        subject: Opaque identifier of the holder, such as an identity subject
            or a service account id.
        role: Name of the role; its permissions are looked up in the catalog
            at decision time.
        scope: Where the role applies, including everything below it.
    """

    subject: str
    role: str
    scope: Scope

    def __post_init__(self) -> None:
        """Reject a grant that names no holder, an unusable role name or no scope."""
        if not self.subject:
            raise ValueError("A grant needs a subject.")
        require_name("role", self.role)
        if not isinstance(self.scope, Scope):
            raise TypeError(f"A grant scope must be a Scope, not {type(self.scope).__name__}.")


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

    async def grants_for(self, subject: str) -> Collection[Grant]:
        """Return the grants of *subject*, in the order they were given.

        Args:
            subject: Identifier of the holder.

        Returns:
            The subject's grants.
        """
        return tuple(grant for grant in self._grants if grant.subject == subject)
