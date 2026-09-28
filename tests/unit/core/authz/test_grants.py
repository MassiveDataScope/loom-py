from __future__ import annotations

from collections.abc import Collection

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
    with pytest.raises(ValueError):
        Grant(subject, role, Scope.root())


def test_rejects_a_scope_given_as_text() -> None:
    with pytest.raises(TypeError, match="Scope"):
        Grant("ada", "viewer", "/T")  # type: ignore[arg-type]


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
    with pytest.raises(ConnectionError, match="unreachable"):
        await _load(_BrokenSource(), "ada")
