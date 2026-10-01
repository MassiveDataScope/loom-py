"""Permissions and roles a product declares in code, and their catalog."""

from __future__ import annotations

import hashlib
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


@dataclass(frozen=True, slots=True)
class RoleSpec:
    """A role declared as data: its own permissions plus the roles it extends.

    Attributes:
        permissions: Names of the permissions the role adds.
        extends: Names of the roles whose permissions it inherits.
    """

    permissions: frozenset[str] = frozenset()
    extends: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CompositionRules:
    """What a product allows custom roles to do on top of its base catalog.

    Attributes:
        custom_prefix: Prefix of every custom role name.
        may_extend_base: Whether a custom role may extend a base role at all.
        non_extensible: Base roles no custom role may extend.
        ceiling: The most a custom role may hold; the product passes the
            creator's effective permissions at the boundary, never ``None``.
    """

    custom_prefix: str
    may_extend_base: bool
    non_extensible: frozenset[str]
    ceiling: frozenset[Permission]

    def __post_init__(self) -> None:
        """Reject a missing ceiling: an open ceiling would let a custom role escalate."""
        if self.ceiling is None:
            raise ValueError("ceiling is required: pass the creator's permissions, never None.")


def _flatten(
    specs: Mapping[str, RoleSpec],
    declared: Mapping[str, Permission],
    bases: Mapping[str, frozenset[Permission]],
) -> dict[str, frozenset[Permission]]:
    resolved: dict[str, frozenset[Permission]] = {}
    visiting: list[str] = []

    def resolve(name: str) -> frozenset[Permission]:
        if name in resolved:
            return resolved[name]
        if name in bases:
            return bases[name]
        if name in visiting:
            cycle = " -> ".join(repr(step) for step in [*visiting[visiting.index(name) :], name])
            raise ValueError(f"Role extends form a cycle: {cycle}.")
        visiting.append(name)
        permissions: set[Permission] = set()
        for base in specs[name].extends:
            if base not in specs and base not in bases:
                raise ValueError(f"Role {name!r} extends unknown role {base!r}.")
            permissions |= resolve(base)
        for permission in sorted(specs[name].permissions):
            if permission not in declared:
                raise ValueError(f"Role {name!r} uses undeclared permission {permission}.")
            permissions.add(declared[permission])
        visiting.pop()
        resolved[name] = frozenset(permissions)
        return resolved[name]

    for name in specs:
        resolve(name)
    return resolved


def _require_segment(kind: str, value: str) -> None:
    if not value or "/" in value:
        raise ValueError(f"A {kind} must be non-empty and contain no '/': {value!r}.")


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

    @classmethod
    def from_specs(
        cls, permissions: Iterable[Permission], specs: Mapping[str, RoleSpec]
    ) -> RoleCatalog:
        """Build a catalog from declarative specs, flattening ``extends``.

        Args:
            permissions: Every permission the product declares.
            specs: Role name to spec, in declaration order.

        Returns:
            The catalog, equal to one built from the flattened roles.

        Raises:
            ValueError: On a cycle, an unknown base or an undeclared permission.
        """
        declared = {permission.name: permission for permission in permissions}
        flattened = _flatten(specs, declared, {})
        return cls(declared.values(), [Role(name, flattened[name]) for name in specs])

    @classmethod
    def compose(
        cls,
        base: RoleCatalog,
        custom: Mapping[str, RoleSpec],
        rules: CompositionRules,
        *,
        namespace: str,
    ) -> RoleCatalog:
        """Add custom roles to a base catalog under the product's rules.

        A custom role is named ``f"{rules.custom_prefix}{namespace}/{name}"``,
        may only combine declared permissions, never exceeds the ceiling,
        never replaces a base role and never extends a non-extensible one.

        Args:
            base: The fixed catalog the product ships.
            custom: Custom role name to spec.
            rules: What custom roles may do.
            namespace: Where the custom roles belong, such as a boundary id.

        Returns:
            A catalog with the base roles, unchanged, plus the custom ones.

        Raises:
            ValueError: When the namespace, a name, an extension or a
                permission breaks the rules; the message names the offender.
        """
        _require_segment("namespace", namespace)
        base_roles = {role.name: role.permissions for role in base.roles}
        declared = {permission.name: permission for permission in base.permissions}
        for name, spec in custom.items():
            _require_segment("custom role name", name)
            for extended in spec.extends:
                if extended not in base_roles:
                    continue
                if not rules.may_extend_base:
                    raise ValueError(f"Custom role {name!r} may not extend base roles.")
                if extended in rules.non_extensible:
                    raise ValueError(f"Base role {extended!r} is not extensible.")
        flattened = _flatten(custom, declared, base_roles)
        roles: list[Role] = list(base.roles)
        for name in custom:
            above = sorted(permission.name for permission in flattened[name] - rules.ceiling)
            if above:
                raise ValueError(f"Custom role {name!r} exceeds the ceiling: {', '.join(above)}.")
            effective = f"{rules.custom_prefix}{namespace}/{name}"
            if effective in base_roles:
                raise ValueError(
                    f"A base role is already named {effective!r}; custom roles never replace it."
                )
            roles.append(Role(effective, flattened[name]))
        return cls(base.permissions, roles)

    def digest(self) -> str:
        """Return a SHA-256 over the sorted ``role:perm,perm`` lines of the catalog."""
        lines = sorted(
            f"{role.name}:{','.join(sorted(p.name for p in role.permissions))}\n"
            for role in self._roles.values()
        )
        return hashlib.sha256("".join(lines).encode()).hexdigest()

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
