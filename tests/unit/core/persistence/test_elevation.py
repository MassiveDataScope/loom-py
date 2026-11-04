"""Elevation across stores: D2 at startup, and one sink routing each flag to its store."""

from __future__ import annotations

from collections.abc import Awaitable, Iterator
from dataclasses import dataclass, field

import pytest

from loom.core.authz import Permission, Role, RoleCatalog
from loom.core.authz.product import clear_authz_product, register_authz_product
from loom.core.config import ConfigError
from loom.core.model.privilege import READ_WRITE
from loom.core.model.scoped import ScopeColumn, ScopedTable
from loom.core.persistence.elevation import (
    ElevableScopeInSeveralStores,
    ElevationErrorCode,
    ScopedElevationSink,
    ScopedTables,
    scoped_elevation_sink,
    validate_elevations,
)
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from tests.helpers.issue_catalogue import IssueCall, check_renders, issue_calls

pytestmark = pytest.mark.timeout(10)

MANAGE = Permission("members.manage")
AUDIT = StoreKey("audit")
HOLDER = ScopeColumn("holder", "owner_id", "both", elevable=False)
EDITOR = ScopeColumn("editor", "editor", "write", elevable=True)
EDITOR_NOT_ELEVABLE = ScopeColumn("editor", "editor", "write", elevable=False)
REVIEWER = ScopeColumn("reviewer", "reviewer", "write", elevable=True)


def _store(*columns: ScopeColumn, table: str = "rows") -> ScopedTables:
    return {(None, table): ScopedTable(None, table, (HOLDER, *columns), READ_WRITE)}


@dataclass
class _Product:
    elevations: dict[str, Permission] = field(default_factory=lambda: {"editor": MANAGE})
    catalog: RoleCatalog = RoleCatalog((MANAGE,), (Role("admin", (MANAGE,)),))
    delegate: Permission = MANAGE


@pytest.fixture(autouse=True)
def _no_product() -> Iterator[None]:
    clear_authz_product()
    yield
    clear_authz_product()


class TestValidateElevations:
    def test_an_elevable_scope_named_by_the_product_is_accepted(self) -> None:
        validate_elevations(_Product(), {DEFAULT_STORE: _store(EDITOR)})

    @pytest.mark.parametrize(
        ("elevations", "message"),
        [
            ({"nope": MANAGE}, r"unknown scope 'nope'"),
            ({"holder": MANAGE}, r"'holder', which is not elevable"),
        ],
        ids=["unknown", "not-elevable"],
    )
    def test_a_scope_the_product_cannot_elevate_is_refused(
        self, elevations: dict[str, Permission], message: str
    ) -> None:
        product = _Product(elevations=elevations)
        stores = {DEFAULT_STORE: _store(EDITOR)}

        with pytest.raises(ConfigError, match=message):
            validate_elevations(product, stores)

    def test_a_scope_elevable_in_another_store_is_known_to_the_product(self) -> None:
        validate_elevations(_Product(), {DEFAULT_STORE: _store(), AUDIT: _store(EDITOR)})

    @pytest.mark.parametrize(
        ("other", "elevations"),
        [
            (EDITOR, {"editor": MANAGE}),
            (EDITOR_NOT_ELEVABLE, {"editor": MANAGE}),
            (EDITOR, {}),
        ],
        ids=["elevable-in-both", "declared-in-the-other", "elevable-but-not-named"],
    )
    def test_an_elevated_scope_declared_by_two_stores_is_refused(
        self, other: ScopeColumn, elevations: dict[str, Permission]
    ) -> None:
        product = _Product(elevations=elevations)
        stores = {DEFAULT_STORE: _store(EDITOR), AUDIT: _store(other, table="entries")}

        with pytest.raises(ElevableScopeInSeveralStores) as refused:
            validate_elevations(product, stores)

        assert refused.value.issue.code is ElevationErrorCode.ELEVABLE_SCOPE_IN_SEVERAL_STORES
        assert refused.value.issue.field == "editor"
        assert "stores audit, default all declare it" in str(refused.value)

    def test_a_shared_scope_that_is_never_elevated_is_allowed(self) -> None:
        stores = {DEFAULT_STORE: _store(EDITOR), AUDIT: _store(table="entries")}

        validate_elevations(_Product(), stores)

    def test_a_named_scope_declared_but_elevable_nowhere_is_not_elevable_first(self) -> None:
        product = _Product(elevations={"holder": MANAGE})
        stores = {DEFAULT_STORE: _store(EDITOR), AUDIT: _store(table="entries")}

        with pytest.raises(ConfigError, match="not elevable") as refused:
            validate_elevations(product, stores)

        assert not isinstance(refused.value, ElevableScopeInSeveralStores)


class TestTheCatalogue:
    @pytest.mark.parametrize("code", list(ElevationErrorCode), ids=str)
    def test_every_code_renders_from_exactly_its_declared_params(
        self, code: ElevationErrorCode
    ) -> None:
        check_renders(code)

    @pytest.mark.parametrize(
        "call", issue_calls(ElevationErrorCode), ids=lambda call: call.location
    )
    def test_every_call_site_passes_exactly_the_params_of_its_code(self, call: IssueCall) -> None:
        assert call.keywords == ElevationErrorCode[call.member].params


async def _done() -> None:
    return None


@dataclass
class _Store:
    """One store's elevation, recording what the sink asks of it."""

    tables: ScopedTables
    open: bool = False
    calls: list[tuple[str, frozenset[str]]] = field(default_factory=list)

    def scoped_tables(self) -> ScopedTables:
        return self.tables

    def in_transaction(self) -> bool:
        return self.open

    def set_flag(self, scope: str) -> Awaitable[object]:
        self.calls.append(("set", frozenset({scope})))
        return _done()

    def clear_flags(self, scopes: frozenset[str]) -> Awaitable[object]:
        self.calls.append(("clear", scopes))
        return _done()

    def invalidate(self) -> Awaitable[object]:
        self.calls.append(("invalidate", frozenset()))
        return _done()


@dataclass
class _TwoStores:
    default: _Store
    audit: _Store
    sink: ScopedElevationSink


def _two_stores(*, default_open: bool, audit_open: bool) -> _TwoStores:
    """``editor`` elevable in the default store, ``reviewer`` in the audit store."""
    default = _Store(_store(EDITOR), open=default_open)
    audit = _Store(_store(REVIEWER, table="entries"), open=audit_open)
    sink = ScopedElevationSink(
        {DEFAULT_STORE: default, AUDIT: audit},
        {DEFAULT_STORE: frozenset({"editor"}), AUDIT: frozenset({"reviewer"})},
    )
    return _TwoStores(default, audit, sink)


class TestScopedElevationSink:
    @pytest.mark.parametrize(
        ("default_open", "audit_open", "scopes", "expected"),
        [
            (True, False, {"editor"}, True),
            (True, False, {"reviewer"}, False),
            (False, True, {"editor"}, False),
            (False, True, {"reviewer"}, True),
            (False, True, {"editor", "reviewer"}, True),
            (True, True, {"holder"}, False),
            (False, False, {"editor", "reviewer"}, False),
        ],
    )
    def test_in_transaction_asks_only_the_stores_declaring_the_scopes(
        self, default_open: bool, audit_open: bool, scopes: set[str], expected: bool
    ) -> None:
        stores = _two_stores(default_open=default_open, audit_open=audit_open)

        assert stores.sink.in_transaction(frozenset(scopes)) is expected

    async def test_a_flag_is_set_on_the_declaring_store_only(self) -> None:
        stores = _two_stores(default_open=True, audit_open=True)

        await stores.sink.set_flag("reviewer")

        assert (stores.default.calls, stores.audit.calls) == (
            [],
            [("set", frozenset({"reviewer"}))],
        )

    async def test_each_store_clears_its_own_scopes(self) -> None:
        stores = _two_stores(default_open=True, audit_open=True)

        await stores.sink.clear_flags(frozenset({"editor", "reviewer"}))

        assert (stores.default.calls, stores.audit.calls) == (
            [("clear", frozenset({"editor"}))],
            [("clear", frozenset({"reviewer"}))],
        )

    async def test_a_store_without_a_transaction_is_left_alone(self) -> None:
        stores = _two_stores(default_open=True, audit_open=False)

        await stores.sink.clear_flags(frozenset({"editor", "reviewer"}))
        await stores.sink.invalidate(frozenset({"editor", "reviewer"}))

        assert (stores.default.calls, stores.audit.calls) == (
            [("clear", frozenset({"editor"})), ("invalidate", frozenset())],
            [],
        )


class TestScopedElevationSinkFactory:
    async def test_it_routes_by_each_store_s_elevable_scopes(self) -> None:
        default = _Store(_store(EDITOR), open=True)
        audit = _Store(_store(REVIEWER, table="entries"), open=True)
        sink = scoped_elevation_sink({DEFAULT_STORE: default, AUDIT: audit})

        await sink.set_flag("editor")

        assert (default.calls, audit.calls) == ([("set", frozenset({"editor"}))], [])

    def test_it_checks_the_registered_product_against_every_store(self) -> None:
        register_authz_product(_Product())
        stores = {
            DEFAULT_STORE: _Store(_store(EDITOR)),
            AUDIT: _Store(_store(EDITOR, table="entries")),
        }

        with pytest.raises(ElevableScopeInSeveralStores):
            scoped_elevation_sink(stores)
