"""Pure decisions over a subject's grants: access, listing and delegation."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from loom.core.authz._grants import Grant
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


def _specificity(grant: Grant) -> tuple[int, str, str]:
    return (-len(grant.scope.path), grant.role, grant.subject)


def evaluate(
    catalog: RoleCatalog,
    grants: Iterable[Grant],
    permission: Permission,
    scope: Scope,
) -> Decision:
    """Decide whether a subject holds *permission* on *scope*.

    Denies unless one of *grants* names a role that includes *permission* on a
    scope covering *scope*.  A grant whose role the catalog does not declare
    allows nothing.  When several grants allow, the decision names the one on
    the deepest scope, then the first by role name and subject, regardless of
    the order of *grants*.

    Args:
        catalog: The product's roles.
        grants: The grants of one subject.
        permission: The capability being exercised.
        scope: Where it is exercised.

    Returns:
        The decision, carrying the grant that allowed it.

    Raises:
        UnknownPermission: When *permission* is not declared in the catalog.
    """
    roles = catalog.roles_with(permission)
    covering = {scope.path[:depth] for depth in range(len(scope.path) + 1)}
    winner: Grant | None = None
    for grant in grants:
        if (
            grant.role in roles
            and grant.scope.path in covering
            and (winner is None or _specificity(grant) < _specificity(winner))
        ):
            winner = grant
    return Decision(winner is not None, winner)


def scopes_with(
    catalog: RoleCatalog,
    grants: Iterable[Grant],
    permission: Permission,
) -> frozenset[Scope]:
    """Return the outermost scopes on which a subject holds *permission*.

    A scope covered by another returned scope is left out, so a listing can
    be filtered with one :meth:`Scope.covers` test per returned scope.

    Args:
        catalog: The product's roles.
        grants: The grants of one subject.
        permission: The capability being listed.

    Returns:
        The minimal set of scopes covering everything the subject may reach.

    Raises:
        UnknownPermission: When *permission* is not declared in the catalog.
    """
    roles = catalog.roles_with(permission)
    held = {grant.scope for grant in grants if grant.role in roles}
    return frozenset(
        scope for scope in held if not any(other != scope and other.covers(scope) for other in held)
    )


def _delegation_check(
    catalog: RoleCatalog,
    granter_grants: Iterable[Grant],
    needed: Iterable[Permission],
    scope: Scope,
) -> GrantCheck:
    held = tuple(granter_grants)
    missing = frozenset(
        permission for permission in needed if not evaluate(catalog, held, permission, scope)
    )
    return GrantCheck(not missing, missing)


def can_grant(
    catalog: RoleCatalog,
    granter_grants: Iterable[Grant],
    role: str,
    scope: Scope,
    *,
    delegate: Permission,
) -> GrantCheck:
    """Decide whether a granter may assign *role* on *scope*.

    The granter needs *delegate* and every permission of *role*, each held on
    a scope covering *scope*.

    Args:
        catalog: The product's roles.
        granter_grants: The granter's grants.
        role: Name of the role to assign.
        scope: Where it would apply.
        delegate: The permission that allows assigning roles at all.

    Returns:
        The check, listing the permissions the granter lacks there.

    Raises:
        UnknownRole: When *role* is not declared in the catalog.
        UnknownPermission: When *delegate* is not declared in the catalog.
    """
    needed = {delegate, *catalog.role(role).permissions}
    return _delegation_check(catalog, granter_grants, needed, scope)


def can_revoke(
    catalog: RoleCatalog,
    granter_grants: Iterable[Grant],
    grant: Grant,
    *,
    delegate: Permission,
) -> GrantCheck:
    """Decide whether a granter may remove *grant*.

    Revoking needs what granting it would.  A grant whose role the code does
    not declare allows nothing, so removing it needs *delegate* only.

    Args:
        catalog: The product's roles.
        granter_grants: The granter's grants.
        grant: The assignment to remove.
        delegate: The permission that allows assigning roles at all.

    Returns:
        The check, listing the permissions the granter lacks there.

    Raises:
        UnknownPermission: When *delegate* is not declared in the catalog.
    """
    needed = {delegate, *(catalog.permissions_of(grant.role) or frozenset())}
    return _delegation_check(catalog, granter_grants, needed, grant.scope)
