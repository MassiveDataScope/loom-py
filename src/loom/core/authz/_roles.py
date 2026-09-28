"""Permissions and roles a product declares in code, and their catalog."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass


def require_name(kind: str, name: str) -> None:
    """Reject a name that cannot be written unambiguously in code and storage.

    Args:
        kind: What is being named, for the error message.
        name: The name to check.

    Raises:
        ValueError: When *name* is empty or contains whitespace.
    """
    if not name or any(character.isspace() for character in name):
        raise ValueError(f"A {kind} name must be non-empty and contain no whitespace: {name!r}.")


class UnknownRole(LookupError):
    """Raised when a role name is not declared in the catalog."""


class UnknownPermission(LookupError):
    """Raised when a permission is not declared in the catalog."""


@dataclass(frozen=True, slots=True, order=True)
class Permission:
    """A capability the product protects, such as ``"catalog.read"``.

    Attributes:
        name: Stable identifier of the capability.
    """

    name: str

    def __post_init__(self) -> None:
        """Reject names that cannot be stored unambiguously."""
        require_name("permission", self.name)


@dataclass(frozen=True, slots=True, init=False)
class Role:
    """A named set of permissions.

    Grants store the role name only, so changing a role here changes what
    every stored grant of it allows.

    Attributes:
        name: Stable identifier of the role.
        permissions: What the role allows.
    """

    name: str
    permissions: frozenset[Permission]

    def __init__(self, name: str, permissions: Iterable[Permission] = ()) -> None:
        """Create a role.

        Args:
            name: Stable identifier of the role.
            permissions: What the role allows; copied, so later changes to
                the argument do not reach the role.

        Raises:
            ValueError: When *name* is empty or contains whitespace.
        """
        require_name("role", name)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "permissions", frozenset(permissions))


class RoleCatalog:
    """Every permission and role a product declares, validated once.

    Example::

        read, operate = Permission("catalog.read"), Permission("etl.operate")
        catalog = RoleCatalog(
            [read, operate],
            [Role("viewer", {read}), Role("operator", {read, operate})],
        )
    """

    __slots__ = ("_holders", "_permissions", "_roles")

    def __init__(self, permissions: Iterable[Permission], roles: Iterable[Role]) -> None:
        """Validate and index the product's roles.

        Args:
            permissions: Every permission the product declares.
            roles: Every role the product declares.

        Raises:
            ValueError: When two roles share a name or a role uses an
                undeclared permission.
        """
        declared = frozenset(permissions)
        indexed: dict[str, Role] = {}
        for role in roles:
            if role.name in indexed:
                raise ValueError(f"Role {role.name!r} is declared twice.")
            undeclared = sorted(permission.name for permission in role.permissions - declared)
            if undeclared:
                raise ValueError(
                    f"Role {role.name!r} uses undeclared permissions: {', '.join(undeclared)}."
                )
            indexed[role.name] = role
        self._permissions = declared
        self._roles: Mapping[str, Role] = indexed
        self._holders: Mapping[Permission, frozenset[str]] = {
            permission: frozenset(
                name for name, role in indexed.items() if permission in role.permissions
            )
            for permission in declared
        }

    @property
    def permissions(self) -> frozenset[Permission]:
        """Every declared permission."""
        return self._permissions

    @property
    def roles(self) -> tuple[Role, ...]:
        """Every declared role, in declaration order."""
        return tuple(self._roles.values())

    def role(self, name: str) -> Role:
        """Return the role declared as *name*.

        Args:
            name: Role name.

        Returns:
            The declared role.

        Raises:
            UnknownRole: When no role has that name.
        """
        try:
            return self._roles[name]
        except KeyError:
            raise UnknownRole(f"Role {name!r} is not declared in the catalog.") from None

    def permissions_of(self, name: str) -> frozenset[Permission] | None:
        """Return the permissions of role *name*, or ``None`` when undeclared.

        Args:
            name: Role name.

        Returns:
            The role's permissions, or ``None`` for a role the code does not
            declare.
        """
        role = self._roles.get(name)
        return None if role is None else role.permissions

    def roles_with(self, permission: Permission) -> frozenset[str]:
        """Return the names of the roles that include *permission*.

        Args:
            permission: A declared permission.

        Returns:
            The role names; empty when no role includes it.

        Raises:
            UnknownPermission: When *permission* is not declared.
        """
        try:
            return self._holders[permission]
        except KeyError:
            raise UnknownPermission(
                f"Permission {permission.name!r} is not declared in the catalog."
            ) from None

    def require(self, permission: Permission) -> None:
        """Reject a permission the product never declared.

        Args:
            permission: The permission about to be checked.

        Raises:
            UnknownPermission: When *permission* is not declared.
        """
        if permission not in self._permissions:
            raise UnknownPermission(
                f"Permission {permission.name!r} is not declared in the catalog."
            )
