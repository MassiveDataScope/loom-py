from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.core.authz import (
    Decision,
    Grant,
    GrantCheck,
    Permission,
    Role,
    RoleCatalog,
    Scope,
    UnknownPermission,
    UnknownRole,
    can_grant,
    can_revoke,
    evaluate,
    scopes_with,
)

READ = Permission("catalog.read")
OPERATE = Permission("etl.operate")
MANAGE = Permission("members.manage")
UNDECLARED = Permission("billing.pay")

CATALOG = RoleCatalog(
    [READ, OPERATE, MANAGE],
    [
        Role("viewer", {READ}),
        Role("operator", {READ, OPERATE}),
        Role("admin", {READ, OPERATE, MANAGE}),
        Role("member_manager", {READ, MANAGE}),
    ],
)

ROOT = Scope.root()
T = Scope.of("T")
U = Scope.of("U")
R = T.child("sales", "orders")
R_SIBLING = T.child("sales", "customers")


def _grants(role: str, scope: Scope, subject: str = "ada") -> list[Grant]:
    return [Grant(subject, role, scope)]


class TestEvaluate:
    def test_no_grants_denies(self) -> None:
        assert evaluate(CATALOG, [], READ, T) == Decision(False, None)

    def test_grant_on_the_scope_allows(self) -> None:
        grants = _grants("viewer", T)

        assert evaluate(CATALOG, grants, READ, T) == Decision(True, grants[0])

    def test_grant_covers_everything_below(self) -> None:
        assert evaluate(CATALOG, _grants("viewer", T), READ, R)

    def test_grant_below_does_not_cover_above_or_siblings(self) -> None:
        grants = _grants("viewer", R)

        assert not evaluate(CATALOG, grants, READ, T)
        assert not evaluate(CATALOG, grants, READ, R_SIBLING)

    def test_root_grant_covers_any_scope(self) -> None:
        assert evaluate(CATALOG, _grants("viewer", ROOT), READ, U)

    def test_grant_does_not_cross_to_another_branch(self) -> None:
        assert not evaluate(CATALOG, _grants("viewer", T), READ, U)

    def test_roles_add_up(self) -> None:
        grants = [Grant("ada", "viewer", T), Grant("ada", "operator", R)]

        assert evaluate(CATALOG, grants, OPERATE, R)
        assert not evaluate(CATALOG, grants, OPERATE, T)
        assert evaluate(CATALOG, grants, READ, T)

    def test_decision_names_the_most_specific_grant_whatever_the_order(self) -> None:
        broad = Grant("ada", "admin", ROOT)
        middle = Grant("ada", "viewer", T)
        narrow = Grant("ada", "operator", R)

        for order in ([broad, middle, narrow], [narrow, broad, middle], [middle, narrow, broad]):
            assert evaluate(CATALOG, order, READ, R).grant == narrow

    def test_ties_on_depth_resolve_by_role_name(self) -> None:
        grants = [Grant("ada", "viewer", T), Grant("ada", "operator", T)]

        assert evaluate(CATALOG, grants, READ, T).grant == Grant("ada", "operator", T)

    def test_permission_outside_the_role_denies(self) -> None:
        assert not evaluate(CATALOG, _grants("viewer", ROOT), OPERATE, T)

    def test_stale_role_grants_nothing(self) -> None:
        assert not evaluate(CATALOG, _grants("retired", ROOT), READ, T)

    def test_undeclared_permission_is_a_programming_error(self) -> None:
        with pytest.raises(UnknownPermission, match="billing.pay"):
            evaluate(CATALOG, _grants("admin", ROOT), UNDECLARED, T)

    def test_accepts_a_one_shot_iterable(self) -> None:
        assert evaluate(CATALOG, iter(_grants("viewer", T)), READ, R)


class TestScopesWith:
    def test_returns_the_minimal_cover(self) -> None:
        other = U.child("x")
        grants = [
            Grant("ada", "viewer", T),
            Grant("ada", "operator", R),
            Grant("ada", "viewer", other),
        ]

        assert scopes_with(CATALOG, grants, READ) == frozenset({T, other})
        assert scopes_with(CATALOG, grants, OPERATE) == frozenset({R})

    def test_root_absorbs_everything(self) -> None:
        grants = [Grant("ada", "viewer", ROOT), Grant("ada", "viewer", T)]

        assert scopes_with(CATALOG, grants, READ) == frozenset({ROOT})

    def test_without_the_permission_is_empty(self) -> None:
        assert scopes_with(CATALOG, _grants("viewer", T), MANAGE) == frozenset()

    def test_undeclared_permission_is_a_programming_error(self) -> None:
        with pytest.raises(UnknownPermission):
            scopes_with(CATALOG, [], UNDECLARED)


class TestCanGrant:
    def test_may_grant_what_it_holds_on_that_scope(self) -> None:
        check = can_grant(CATALOG, _grants("admin", T), "admin", T, delegate=MANAGE)

        assert check == GrantCheck(True, frozenset())

    def test_may_grant_below_its_scope(self) -> None:
        assert can_grant(CATALOG, _grants("admin", T), "operator", R, delegate=MANAGE)

    def test_may_not_grant_elsewhere_or_above(self) -> None:
        grants = _grants("admin", T)

        assert not can_grant(CATALOG, grants, "admin", U, delegate=MANAGE)
        assert not can_grant(CATALOG, grants, "admin", ROOT, delegate=MANAGE)

    def test_lists_the_missing_permissions(self) -> None:
        check = can_grant(CATALOG, _grants("member_manager", T), "admin", T, delegate=MANAGE)

        assert check == GrantCheck(False, frozenset({OPERATE}))

    def test_holding_the_role_is_not_enough_without_the_delegate_permission(self) -> None:
        check = can_grant(CATALOG, _grants("operator", T), "viewer", T, delegate=MANAGE)

        assert check == GrantCheck(False, frozenset({MANAGE}))

    def test_root_holder_may_grant_anywhere(self) -> None:
        assert can_grant(CATALOG, _grants("admin", ROOT), "admin", U, delegate=MANAGE)

    def test_unknown_role_is_a_programming_error(self) -> None:
        with pytest.raises(UnknownRole, match="ghost"):
            can_grant(CATALOG, _grants("admin", ROOT), "ghost", T, delegate=MANAGE)


class TestCanRevoke:
    def test_may_revoke_what_it_could_grant(self) -> None:
        target = Grant("bob", "operator", R)

        assert can_revoke(CATALOG, _grants("admin", T), target, delegate=MANAGE)

    def test_may_not_revoke_what_it_could_not_grant(self) -> None:
        target = Grant("bob", "operator", T)
        check = can_revoke(CATALOG, _grants("member_manager", T), target, delegate=MANAGE)

        assert check == GrantCheck(False, frozenset({OPERATE}))

    def test_a_stale_role_needs_only_the_delegate_permission(self) -> None:
        stale = Grant("bob", "retired", T)

        assert can_revoke(CATALOG, _grants("member_manager", T), stale, delegate=MANAGE)
        assert not can_revoke(CATALOG, _grants("viewer", T), stale, delegate=MANAGE)
        assert not can_revoke(CATALOG, _grants("admin", U), stale, delegate=MANAGE)


@pytest.mark.parametrize(
    "build",
    [
        lambda: Decision(True, None),
        lambda: Decision(False, Grant("ada", "viewer", ROOT)),
        lambda: GrantCheck(True, frozenset({READ})),
        lambda: GrantCheck(False, frozenset()),
    ],
)
def test_outcomes_cannot_contradict_themselves(build: Callable[[], object]) -> None:
    with pytest.raises(ValueError):
        build()
