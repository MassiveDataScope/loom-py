"""Pure decisions over a subject's grants: access, listing and delegation."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from loom.core.authz._grants import Grant, require_aware
from loom.core.authz._roles import Permission, RoleCatalog
from loom.core.authz._scope import Scope


@dataclass(frozen=True, slots=True)
class Decision:
    """Outcome of an access check, truthy when allowed.

    Attributes:
        allowed: Whether the permission is held on the scope.
        grant: The grant that allowed it; ``None`` when denied.
    """

    allowed: bool
    grant: Grant | None

    def __post_init__(self) -> None:
        """Reject a decision whose grant disagrees with :attr:`allowed`."""
        if self.allowed != (self.grant is not None):
            raise ValueError("A decision names a grant exactly when it allows.")

    def __bool__(self) -> bool:
        """Return :attr:`allowed`."""
        return self.allowed


@dataclass(frozen=True, slots=True)
class GrantCheck:
    """Outcome of a delegation check, truthy when allowed.

    Attributes:
        allowed: Whether the granter may hand out or remove the role there.
        missing: Permissions the granter lacks on that scope.
    """

    allowed: bool
    missing: frozenset[Permission]

    def __post_init__(self) -> None:
        """Reject a check whose :attr:`allowed` disagrees with :attr:`missing`."""
        if self.allowed == bool(self.missing):
            raise ValueError("A grant check allows exactly when nothing is missing.")

    def __bool__(self) -> bool:
        """Return :attr:`allowed`."""
        return self.allowed


_FOREVER = float("-inf")


def _rank(grant: Grant) -> tuple[int, str, str, float]:
    lifetime = _FOREVER if grant.expires_at is None else -grant.expires_at.timestamp()
    return (-len(grant.scope.path), grant.role, grant.subject, lifetime)


def _check_now(now: datetime | None) -> None:
    if now is not None:
        require_aware("now", now)


def _require_now_for_expiring(grants: tuple[Grant, ...], now: datetime | None) -> None:
    expiry = next((grant.expires_at for grant in grants if grant.expires_at is not None), None)
    if now is None and expiry is not None:
        raise ValueError(
            f"A grant expires at {expiry.isoformat()}; pass now= to decide whether it is alive."
        )


def _alive(grant: Grant, now: datetime | None) -> bool:
    return grant.expires_at is None or (now is not None and now < grant.expires_at)


def _covering_paths(scope: Scope) -> frozenset[tuple[str, ...]]:
    return frozenset(scope.path[:depth] for depth in range(len(scope.path) + 1))


def _outermost(scopes: set[Scope]) -> frozenset[Scope]:
    return frozenset(
        scope
        for scope in scopes
        if not any(other != scope and other.covers(scope) for other in scopes)
    )


def evaluate(
    catalog: RoleCatalog,
    grants: Iterable[Grant],
    permission: Permission,
    scope: Scope,
    *,
    now: datetime | None = None,
) -> Decision:
    """Decide whether a subject holds *permission* on *scope*.

    Denies unless one of *grants* names a role that includes *permission* on a
    scope covering *scope*.  A grant whose role the catalog does not declare
    allows nothing.  When several grants allow, the decision names the one on
    the deepest scope, then the first by role name and subject, regardless of
    the order of *grants*; among grants that tie there, the one that lives
    longest.  A grant is alive while *now* is before its expiry; an expired
    grant counts as absent.

    Args:
        catalog: The product's roles.
        grants: The grants of one subject.
        permission: The capability being exercised.
        scope: Where it is exercised.
        now: The timezone-aware instant of the decision; needed only when
            some grant carries an expiry.

    Returns:
        The decision, carrying the grant that allowed it.

    Raises:
        UnknownPermission: When *permission* is not declared in the catalog.
        TypeError: When *now* is not a datetime.
        ValueError: When *now* is naive, or is ``None`` while some grant
            carries an expiry.
    """
    _check_now(now)
    held = tuple(grants)
    _require_now_for_expiring(held, now)
    roles = catalog.roles_with(permission)
    covering = _covering_paths(scope)
    allowing = [
        grant
        for grant in held
        if grant.role in roles and grant.scope.path in covering and _alive(grant, now)
    ]
    winner = min(allowing, key=_rank, default=None)
    return Decision(winner is not None, winner)


def scopes_with(
    catalog: RoleCatalog,
    grants: Iterable[Grant],
    permission: Permission,
    *,
    now: datetime | None = None,
) -> frozenset[Scope]:
    """Return the outermost scopes on which a subject holds *permission*.

    A scope covered by another returned scope is left out, so a listing can
    be filtered with one :meth:`Scope.covers` test per returned scope.  An
    expired grant counts as absent.

    Args:
        catalog: The product's roles.
        grants: The grants of one subject.
        permission: The capability being listed.
        now: The timezone-aware instant of the listing; needed only when
            some grant carries an expiry.

    Returns:
        The minimal set of scopes covering everything the subject may reach.

    Raises:
        UnknownPermission: When *permission* is not declared in the catalog.
        TypeError: When *now* is not a datetime.
        ValueError: When *now* is naive, or is ``None`` while some grant
            carries an expiry.
    """
    _check_now(now)
    held = tuple(grants)
    _require_now_for_expiring(held, now)
    roles = catalog.roles_with(permission)
    return _outermost({grant.scope for grant in held if grant.role in roles and _alive(grant, now)})


def _delegation_check(
    catalog: RoleCatalog,
    granter_grants: Iterable[Grant],
    needed: Iterable[Permission],
    scope: Scope,
    now: datetime | None,
) -> GrantCheck:
    held = tuple(granter_grants)
    missing = frozenset(
        permission
        for permission in needed
        if not evaluate(catalog, held, permission, scope, now=now)
    )
    return GrantCheck(not missing, missing)


def can_grant(
    catalog: RoleCatalog,
    granter_grants: Iterable[Grant],
    role: str,
    scope: Scope,
    *,
    delegate: Permission,
    now: datetime | None = None,
) -> GrantCheck:
    """Decide whether a granter may assign *role* on *scope*.

    The granter needs *delegate* and every permission of *role*, each held on
    a scope covering *scope* by a grant that is alive at *now*.

    Args:
        catalog: The product's roles.
        granter_grants: The granter's grants.
        role: Name of the role to assign.
        scope: Where it would apply.
        delegate: The permission that allows assigning roles at all.
        now: The timezone-aware instant of the check; needed only when some
            of the granter's grants carry an expiry.

    Returns:
        The check, listing the permissions the granter lacks there.

    Raises:
        UnknownRole: When *role* is not declared in the catalog.
        UnknownPermission: When *delegate* is not declared in the catalog.
        TypeError: When *now* is not a datetime.
        ValueError: When *now* is naive, or is ``None`` while some of the
            granter's grants carry an expiry.
    """
    _check_now(now)
    needed = {delegate, *catalog.role(role).permissions}
    return _delegation_check(catalog, granter_grants, needed, scope, now)


def can_revoke(
    catalog: RoleCatalog,
    granter_grants: Iterable[Grant],
    grant: Grant,
    *,
    delegate: Permission,
    now: datetime | None = None,
) -> GrantCheck:
    """Decide whether a granter may remove *grant*.

    Revoking needs what granting it would, whether or not *grant* has
    expired.  A grant whose role the code does not declare allows nothing, so
    removing it needs *delegate* only.

    Args:
        catalog: The product's roles.
        granter_grants: The granter's grants.
        grant: The assignment to remove.
        delegate: The permission that allows assigning roles at all.
        now: The timezone-aware instant of the check; needed only when some
            of the granter's grants carry an expiry.

    Returns:
        The check, listing the permissions the granter lacks there.

    Raises:
        UnknownPermission: When *delegate* is not declared in the catalog.
        TypeError: When *now* is not a datetime.
        ValueError: When *now* is naive, or is ``None`` while some of the
            granter's grants carry an expiry.
    """
    _check_now(now)
    needed = {delegate, *(catalog.permissions_of(grant.role) or frozenset())}
    return _delegation_check(catalog, granter_grants, needed, grant.scope, now)
