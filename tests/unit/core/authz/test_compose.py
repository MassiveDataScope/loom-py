"""Composition of a fixed base catalog with custom roles under a ceiling (T021, FR-031)."""

from __future__ import annotations

import pytest

from loom.core.authz import (
    CompositionRules,
    Grant,
    Permission,
    RoleCatalog,
    RoleSpec,
    Scope,
    can_grant,
    evaluate,
)

READ = Permission("catalog.read")
QUERY = Permission("query.run")
OPERATE = Permission("etl.operate")
MANAGE = Permission("members.manage")
ALL = (READ, QUERY, OPERATE, MANAGE)

BASE = RoleCatalog.from_specs(
    ALL,
    {
        "viewer": RoleSpec(permissions=frozenset({"catalog.read"})),
        "admin": RoleSpec(
            permissions=frozenset({"query.run", "etl.operate", "members.manage"}),
            extends=("viewer",),
        ),
        "platform": RoleSpec(permissions=frozenset()),
    },
)


def rules(**overrides: object) -> CompositionRules:
    fields: dict[str, object] = {
        "custom_prefix": "custom:",
        "may_extend_base": True,
        "non_extensible": frozenset({"platform"}),
        "ceiling": frozenset({READ, QUERY, OPERATE}),
    }
    fields.update(overrides)
    return CompositionRules(**fields)  # type: ignore[arg-type]


def test_every_rule_is_required_and_the_ceiling_is_never_none() -> None:
    with pytest.raises(TypeError):
        CompositionRules(custom_prefix="custom:", may_extend_base=True)  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="ceiling"):
        rules(ceiling=None)


def test_a_custom_role_is_named_by_prefix_namespace_and_name() -> None:
    custom = {"analyst": RoleSpec(permissions=frozenset({"query.run"}), extends=("viewer",))}

    catalog = RoleCatalog.compose(BASE, custom, rules(), namespace="acme")

    assert catalog.role("custom:acme/analyst").permissions == {READ, QUERY}
    assert catalog.role("admin") is BASE.role("admin")


def test_a_permission_above_the_ceiling_is_rejected_by_name() -> None:
    custom = {"boss": RoleSpec(permissions=frozenset({"members.manage"}))}

    composition_rules = rules()

    with pytest.raises(ValueError, match="'boss'.*members.manage"):
        RoleCatalog.compose(BASE, custom, composition_rules, namespace="acme")


def test_an_inherited_permission_above_the_ceiling_is_rejected_too() -> None:
    custom = {"shadow": RoleSpec(extends=("admin",))}

    composition_rules = rules()

    with pytest.raises(ValueError, match="'shadow'.*members.manage"):
        RoleCatalog.compose(BASE, custom, composition_rules, namespace="acme")


def test_a_custom_role_cannot_take_a_base_name() -> None:
    base = RoleCatalog.from_specs(
        ALL, {"custom:acme/viewer": RoleSpec(permissions=frozenset({"catalog.read"}))}
    )
    custom = {"viewer": RoleSpec(permissions=frozenset({"catalog.read"}))}

    composition_rules = rules()

    with pytest.raises(ValueError, match="base.*'custom:acme/viewer'"):
        RoleCatalog.compose(base, custom, composition_rules, namespace="acme")


def test_a_non_extensible_base_role_cannot_be_extended() -> None:
    custom = {"ops": RoleSpec(extends=("platform",))}

    composition_rules = rules()

    with pytest.raises(ValueError, match="'platform'.*not extensible"):
        RoleCatalog.compose(BASE, custom, composition_rules, namespace="acme")


def test_extending_base_roles_can_be_disabled_entirely() -> None:
    custom = {"analyst": RoleSpec(extends=("viewer",))}

    composition_rules = rules(may_extend_base=False)

    with pytest.raises(ValueError, match="may not extend base roles"):
        RoleCatalog.compose(BASE, custom, composition_rules, namespace="acme")


def test_the_same_custom_name_in_two_namespaces_yields_two_roles() -> None:
    custom = {"analyst": RoleSpec(permissions=frozenset({"query.run"}))}

    acme = RoleCatalog.compose(BASE, custom, rules(), namespace="acme")
    globex = RoleCatalog.compose(BASE, custom, rules(), namespace="globex")

    assert "custom:acme/analyst" in {role.name for role in acme.roles}
    assert "custom:globex/analyst" in {role.name for role in globex.roles}
    assert acme.permissions_of("custom:globex/analyst") is None


@pytest.mark.parametrize("namespace", ["", "a/b"])
def test_an_empty_or_slashed_namespace_is_rejected(namespace: str) -> None:
    custom = {"analyst": RoleSpec(permissions=frozenset({"query.run"}))}

    composition_rules = rules()

    with pytest.raises(ValueError, match="namespace"):
        RoleCatalog.compose(BASE, custom, composition_rules, namespace=namespace)


@pytest.mark.parametrize("name", ["", "a/b"])
def test_an_empty_or_slashed_custom_name_is_rejected(name: str) -> None:
    custom = {name: RoleSpec(permissions=frozenset({"query.run"}))}

    composition_rules = rules()

    with pytest.raises(ValueError, match="custom role name"):
        RoleCatalog.compose(BASE, custom, composition_rules, namespace="acme")


def test_the_digest_changes_with_a_custom_role() -> None:
    custom = {"analyst": RoleSpec(permissions=frozenset({"query.run"}))}

    composed = RoleCatalog.compose(BASE, custom, rules(), namespace="acme")

    assert composed.digest() != BASE.digest()
    assert RoleCatalog.compose(BASE, {}, rules(), namespace="acme").digest() == BASE.digest()


def test_a_grant_on_a_removed_custom_role_is_inert() -> None:
    grant = Grant("ana", "custom:acme/analyst", Scope.of("acme"))
    with_role = RoleCatalog.compose(
        BASE, {"analyst": RoleSpec(permissions=frozenset({"query.run"}))}, rules(), namespace="acme"
    )
    without_role = RoleCatalog.compose(BASE, {}, rules(), namespace="acme")

    assert evaluate(with_role, [grant], QUERY, Scope.of("acme", "sales"))
    assert not evaluate(without_role, [grant], QUERY, Scope.of("acme", "sales"))


def test_holders_of_an_edited_custom_role_are_rechecked_with_can_grant() -> None:
    """P3: widening a custom role must be allowed for every holder as a fresh grant."""
    granter = [Grant("ana", "admin", Scope.of("acme"))]
    widened = RoleCatalog.compose(
        BASE,
        {"analyst": RoleSpec(permissions=frozenset({"query.run", "etl.operate"}))},
        rules(),
        namespace="acme",
    )
    holder_scope = Scope.of("acme", "sales")

    check = can_grant(widened, granter, "custom:acme/analyst", holder_scope, delegate=MANAGE)

    assert check
    outsider = [Grant("bob", "admin", Scope.of("globex"))]
    assert not can_grant(widened, outsider, "custom:acme/analyst", holder_scope, delegate=MANAGE)
