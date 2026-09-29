from __future__ import annotations

import operator
from collections.abc import Callable, Collection
from datetime import UTC, datetime, timedelta, timezone

import pytest

from loom.core.authz import (
    Grant,
    GrantSource,
    InMemoryGrantSource,
    Permission,
    Role,
    RoleCatalog,
    Scope,
    evaluate,
)

ADA_VIEWER = Grant("ada", "viewer", Scope.of("T"))
BOB_ADMIN = Grant("bob", "admin", Scope.root())


@pytest.mark.parametrize(("subject", "role"), [("", "viewer"), ("ada", ""), ("ada", "two words")])
def test_rejects_empty_subject_or_invalid_role(subject: str, role: str) -> None:
    scope = Scope.root()

    with pytest.raises(ValueError):
        Grant(subject, role, scope)


def test_rejects_a_scope_given_as_text() -> None:
    with pytest.raises(TypeError, match="Scope"):
        Grant("ada", "viewer", "/T")  # type: ignore[arg-type]


def test_rejects_a_naive_expiry() -> None:
    scope = Scope.root()
    naive = datetime(2030, 1, 1)

    with pytest.raises(ValueError, match="timezone-aware"):
        Grant("ada", "viewer", scope, naive)


def test_rejects_an_expiry_that_is_not_a_datetime() -> None:
    scope = Scope.root()

    with pytest.raises(TypeError, match="datetime"):
        Grant("ada", "viewer", scope, "2030-01-01")  # type: ignore[arg-type]


def test_expiry_defaults_to_never() -> None:
    assert ADA_VIEWER.expires_at is None


def test_expiry_takes_part_in_equality_and_hashing() -> None:
    utc = datetime(2030, 1, 1, 12, tzinfo=UTC)
    madrid = utc.astimezone(timezone(timedelta(hours=2)))
    expiring = Grant("ada", "viewer", Scope.of("T"), utc)

    assert expiring == Grant("ada", "viewer", Scope.of("T"), madrid)
    assert hash(expiring) == hash(Grant("ada", "viewer", Scope.of("T"), madrid))
    assert expiring != ADA_VIEWER
    assert len({expiring, ADA_VIEWER}) == 2


def test_orders_grants_that_differ_only_in_expiry() -> None:
    early = Grant("ada", "viewer", Scope.of("T"), datetime(2030, 1, 1, tzinfo=UTC))
    late = Grant("ada", "viewer", Scope.of("T"), datetime(2031, 1, 1, tzinfo=UTC))

    assert sorted([late, ADA_VIEWER, early]) == [ADA_VIEWER, early, late]
    assert sorted([early, late, ADA_VIEWER]) == [ADA_VIEWER, early, late]
    assert ADA_VIEWER < early <= early < late
    assert late > early >= early > ADA_VIEWER


def test_orders_by_subject_role_and_scope_before_expiry() -> None:
    expiring = Grant("ada", "viewer", Scope.of("T"), datetime(2030, 1, 1, tzinfo=UTC))

    assert expiring < BOB_ADMIN
    assert Grant("ada", "admin", Scope.of("Z")) < expiring
    assert expiring < Grant("ada", "viewer", Scope.of("U"))


@pytest.mark.parametrize("compare", [operator.lt, operator.le, operator.gt, operator.ge])
def test_does_not_order_against_other_types(compare: Callable[[object, object], bool]) -> None:
    with pytest.raises(TypeError):
        compare(ADA_VIEWER, "ada")


async def test_in_memory_source_filters_by_subject_keeping_order_without_duplicates() -> None:
    ada_admin = Grant("ada", "admin", Scope.root())
    source = InMemoryGrantSource([ada_admin, BOB_ADMIN, ADA_VIEWER, ada_admin])

    assert await source.grants_for("ada") == (ada_admin, ADA_VIEWER)
    assert await source.grants_for("nobody") == ()


async def test_source_feeds_the_evaluator() -> None:
    read = Permission("catalog.read")
    catalog = RoleCatalog([read], [Role("viewer", {read}), Role("admin", {read})])
    source = InMemoryGrantSource([ADA_VIEWER, BOB_ADMIN])

    assert evaluate(catalog, await source.grants_for("ada"), read, Scope.of("T", "x"))
    assert not evaluate(catalog, await source.grants_for("ada"), read, Scope.of("U"))


class _TableSource:
    async def grants_for(self, subject: str) -> Collection[Grant]:
        return [ADA_VIEWER] if subject == "ada" else []


class _BrokenSource:
    async def grants_for(self, subject: str) -> Collection[Grant]:
        raise ConnectionError("grants store unreachable")


async def _load(source: GrantSource, subject: str) -> Collection[Grant]:
    return await source.grants_for(subject)


async def test_any_class_with_grants_for_is_a_source() -> None:
    assert list(await _load(_TableSource(), "ada")) == [ADA_VIEWER]


async def test_source_errors_propagate() -> None:
    source = _BrokenSource()

    with pytest.raises(ConnectionError, match="unreachable"):
        await _load(source, "ada")


def test_expiry_is_stored_in_utc() -> None:
    tokyo = timezone(timedelta(hours=9))
    grant = Grant("ada", "viewer", Scope.of("T"), datetime(2026, 1, 1, 9, tzinfo=tokyo))

    assert grant.expires_at == datetime(2026, 1, 1, tzinfo=UTC)
    assert grant.expires_at is not None
    assert grant.expires_at.tzinfo is UTC
