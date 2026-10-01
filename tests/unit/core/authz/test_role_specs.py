"""Role catalogs built from declarative specs, and their digest (T020, FR-030)."""

from __future__ import annotations

import itertools

import pytest

from loom.core.authz import Permission, Role, RoleCatalog, RoleSpec

READ = Permission("catalog.read")
QUERY = Permission("query.run")
OPERATE = Permission("etl.operate")
ALL = (READ, QUERY, OPERATE)

SPECS = {
    "viewer": RoleSpec(permissions=frozenset({"catalog.read"})),
    "analyst": RoleSpec(permissions=frozenset({"query.run"}), extends=("viewer",)),
    "operator": RoleSpec(permissions=frozenset({"etl.operate"}), extends=("analyst",)),
}


def test_from_specs_flattens_extends_into_the_same_catalog_as_explicit_roles() -> None:
    explicit = RoleCatalog(
        ALL,
        [
            Role("viewer", {READ}),
            Role("analyst", {READ, QUERY}),
            Role("operator", {READ, QUERY, OPERATE}),
        ],
    )

    built = RoleCatalog.from_specs(ALL, SPECS)

    assert built.roles == explicit.roles
    assert built.digest() == explicit.digest()


def test_from_specs_keeps_declaration_order() -> None:
    catalog = RoleCatalog.from_specs(ALL, SPECS)

    assert [role.name for role in catalog.roles] == ["viewer", "analyst", "operator"]


def test_a_cycle_in_extends_is_rejected_naming_the_roles() -> None:
    specs = {
        "a": RoleSpec(extends=("b",)),
        "b": RoleSpec(extends=("a",)),
    }

    with pytest.raises(ValueError, match="cycle.*'a'.*'b'|cycle.*'b'.*'a'"):
        RoleCatalog.from_specs(ALL, specs)


def test_an_unknown_base_is_rejected_naming_role_and_base() -> None:
    specs = {"analyst": RoleSpec(extends=("ghost",))}

    with pytest.raises(ValueError, match="'analyst'.*'ghost'"):
        RoleCatalog.from_specs(ALL, specs)


def test_an_undeclared_permission_is_rejected_naming_role_and_permission() -> None:
    specs = {"viewer": RoleSpec(permissions=frozenset({"catalog.drop"}))}

    with pytest.raises(ValueError, match="'viewer'.*catalog.drop"):
        RoleCatalog.from_specs(ALL, specs)


def test_digest_does_not_depend_on_declaration_order() -> None:
    digests = set()
    for order in itertools.permutations(SPECS):
        catalog = RoleCatalog.from_specs(ALL, {name: SPECS[name] for name in order})
        digests.add(catalog.digest())

    assert len(digests) == 1


def test_digest_changes_when_one_permission_changes() -> None:
    before = RoleCatalog.from_specs(ALL, SPECS).digest()
    changed = dict(SPECS, viewer=RoleSpec(permissions=frozenset({"catalog.read", "query.run"})))

    after = RoleCatalog.from_specs(ALL, changed).digest()

    assert before != after
    assert len(after) == 64
