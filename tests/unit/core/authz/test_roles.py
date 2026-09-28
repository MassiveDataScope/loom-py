from __future__ import annotations

import pytest

from loom.core.authz import Permission, Role, RoleCatalog, UnknownPermission, UnknownRole

READ = Permission("catalog.read")
OPERATE = Permission("etl.operate")


def test_catalog_resolves_role_permissions() -> None:
    catalog = RoleCatalog(
        [READ, OPERATE], [Role("viewer", {READ}), Role("operator", {READ, OPERATE})]
    )

    assert catalog.permissions_of("operator") == frozenset({READ, OPERATE})
    assert catalog.role("viewer") == Role("viewer", {READ})


def test_unknown_role_yields_none_from_lookup_and_raises_from_role() -> None:
    catalog = RoleCatalog([READ], [Role("viewer", {READ})])

    assert catalog.permissions_of("ghost") is None
    with pytest.raises(UnknownRole, match="ghost"):
        catalog.role("ghost")


def test_rejects_an_undeclared_permission() -> None:
    with pytest.raises(ValueError, match="etl.operate"):
        RoleCatalog([READ], [Role("operator", {READ, OPERATE})])


def test_rejects_a_duplicate_role() -> None:
    with pytest.raises(ValueError, match="viewer"):
        RoleCatalog([READ], [Role("viewer", {READ}), Role("viewer", set())])


@pytest.mark.parametrize("name", ["", "has space", " lead", "tab\tname"])
def test_rejects_invalid_names(name: str) -> None:
    with pytest.raises(ValueError, match="name"):
        Permission(name)
    with pytest.raises(ValueError, match="name"):
        Role(name, set())


def test_role_permissions_are_frozen() -> None:
    permissions = {READ}
    role = Role("viewer", permissions)
    permissions.add(OPERATE)

    assert role.permissions == frozenset({READ})


def test_catalog_lists_what_it_declares() -> None:
    viewer, operator = Role("viewer", {READ}), Role("operator", {READ, OPERATE})
    catalog = RoleCatalog([READ, OPERATE], [viewer, operator])

    assert catalog.roles == (viewer, operator)
    assert catalog.permissions == frozenset({READ, OPERATE})


def test_require_rejects_an_undeclared_permission() -> None:
    catalog = RoleCatalog([READ], [])

    catalog.require(READ)
    with pytest.raises(UnknownPermission, match="etl.operate"):
        catalog.require(OPERATE)


def test_roles_with_names_every_role_that_includes_the_permission() -> None:
    catalog = RoleCatalog(
        [READ, OPERATE], [Role("viewer", {READ}), Role("operator", {READ, OPERATE})]
    )

    assert catalog.roles_with(READ) == frozenset({"viewer", "operator"})
    assert catalog.roles_with(OPERATE) == frozenset({"operator"})
    with pytest.raises(UnknownPermission):
        catalog.roles_with(Permission("billing.pay"))
