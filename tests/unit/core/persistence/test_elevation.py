"""Elevation across stores: D2 at startup, and one sink routing each flag to its store."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Coroutine, Iterator
from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from loom.core.authz import Permission, Role, RoleCatalog
from loom.core.authz.product import (
    clear_authz_product,
    load_authz_product,
    register_authz_product,
)
from loom.core.config import ConfigError
from loom.core.model.privilege import READ_WRITE
from loom.core.model.scoped import ScopeColumn, ScopedTable
from loom.core.persistence.elevation import (
    ElevableScopeInSeveralStores,
    ElevationErrorCode,
    ScopedElevationSink,
    ScopedTables,
    validate_elevations,
)
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from tests.helpers.elevation import MANAGE, Product
from tests.helpers.issue_catalogue import IssueCall, check_renders, issue_calls

pytestmark = pytest.mark.timeout(10)


def test_scoped_elevation_sink_requires_explicit_product() -> None:
    with pytest.raises(TypeError):
        cast(Any, ScopedElevationSink)({})


CATALOG = RoleCatalog((MANAGE,), (Role("admin", (MANAGE,)),))
AUDIT = StoreKey("audit")
HOLDER = ScopeColumn("holder", "owner_id", "both", elevable=False)
EDITOR = ScopeColumn("editor", "editor", "write", elevable=True)
EDITOR_NOT_ELEVABLE = ScopeColumn("editor", "editor", "write", elevable=False)
REVIEWER = ScopeColumn("reviewer", "reviewer", "write", elevable=True)


def _store(*columns: ScopeColumn, table: str = "rows") -> ScopedTables:
    return {(None, table): ScopedTable(None, table, (HOLDER, *columns), READ_WRITE)}


@pytest.fixture(autouse=True)
def _no_product() -> Iterator[None]:
    clear_authz_product()
    yield
    clear_authz_product()


class TestValidateElevations:
    def test_an_elevable_scope_named_by_the_product_is_accepted(self) -> None:
        validate_elevations(Product(CATALOG), {DEFAULT_STORE: _store(), AUDIT: _store(EDITOR)})

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
        product = Product(CATALOG, elevations)
        stores = {DEFAULT_STORE: _store(EDITOR)}

        with pytest.raises(ConfigError, match=message):
            validate_elevations(product, stores)

    def test_an_elevated_scope_declared_by_two_stores_is_refused(self) -> None:
        product = Product(CATALOG)
        stores = {DEFAULT_STORE: _store(EDITOR), AUDIT: _store(EDITOR_NOT_ELEVABLE, table="e")}

        with pytest.raises(ElevableScopeInSeveralStores) as refused:
            validate_elevations(product, stores)

        assert refused.value.issue.code is ElevationErrorCode.ELEVABLE_SCOPE_IN_SEVERAL_STORES
        assert "stores audit, default all declare it" in str(refused.value)

    def test_a_shared_scope_that_is_never_elevated_is_allowed(self) -> None:
        validate_elevations(
            Product(CATALOG), {DEFAULT_STORE: _store(EDITOR), AUDIT: _store(table="e")}
        )


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


@dataclass
class _Store:
    """One store's elevation with a transaction open, recording what the sink asks of it."""

    tables: ScopedTables
    calls: list[tuple[str, frozenset[str]]] = field(default_factory=list)
    clearing: Callable[[], Awaitable[None]] | None = None
    made: list[Coroutine[object, object, None]] = field(default_factory=list)

    def scoped_tables(self) -> ScopedTables:
        return self.tables

    def in_transaction(self) -> bool:
        return True

    def set_flag(self, scope: str) -> Awaitable[object]:
        self.calls.append(("set", frozenset({scope})))
        return _done()

    def clear_flags(self, scopes: frozenset[str]) -> Awaitable[object]:
        self.calls.append(("clear", scopes))
        step = _done() if self.clearing is None else self.clearing()
        if inspect.iscoroutine(step):
            self.made.append(step)
        return step

    def invalidate(self) -> Awaitable[object]:
        self.calls.append(("invalidate", frozenset()))
        return _done()


class _RefusingStore(_Store):
    """A store whose checkpoint refuses the call itself, before any statement is built."""

    def clear_flags(self, scopes: frozenset[str]) -> Awaitable[object]:
        raise RuntimeError("refused before any statement")


async def _done() -> None:
    return None


def _sink(default: _Store, audit: _Store) -> ScopedElevationSink:
    return ScopedElevationSink({DEFAULT_STORE: default, AUDIT: audit}, load_authz_product())


class TestScopedElevationSink:
    async def test_each_store_gets_only_the_scopes_it_declares(self) -> None:
        default, audit = _Store(_store(EDITOR)), _Store(_store(REVIEWER, table="e"))
        sink = _sink(default, audit)

        await sink.set_flag("reviewer")
        await sink.clear_flags(frozenset({"editor", "reviewer"}))

        assert (default.calls, audit.calls) == (
            [("clear", frozenset({"editor"}))],
            [("set", frozenset({"reviewer"})), ("clear", frozenset({"reviewer"}))],
        )

    async def test_a_failing_store_is_raised_once_every_store_finished(self) -> None:
        finished: list[str] = []

        async def fails_fast() -> None:
            raise RuntimeError("clearing refused")

        async def slow() -> None:
            await asyncio.sleep(0.01)
            finished.append("audit")

        default = _Store(_store(EDITOR), clearing=fails_fast)
        audit = _Store(_store(REVIEWER, table="e"), clearing=slow)
        clearing = _sink(default, audit).clear_flags(frozenset({"editor", "reviewer"}))

        with pytest.raises(RuntimeError, match="clearing refused"):
            await clearing

        assert finished == ["audit"]

    async def test_stores_are_cleared_one_after_another(self) -> None:
        running: list[str] = []
        overlapped: list[bool] = []

        def clearing(name: str) -> Callable[[], Awaitable[None]]:
            async def clear() -> None:
                overlapped.append(bool(running))
                running.append(name)
                await asyncio.sleep(0)
                running.remove(name)

            return clear

        default = _Store(_store(EDITOR), clearing=clearing("default"))
        audit = _Store(_store(REVIEWER, table="e"), clearing=clearing("audit"))

        await _sink(default, audit).clear_flags(frozenset({"editor", "reviewer"}))

        assert overlapped == [False, False]

    def test_a_store_refusing_at_call_time_closes_the_calls_already_made(self) -> None:
        default = _Store(_store(EDITOR))
        audit = _RefusingStore(_store(REVIEWER, table="e"))
        sink = _sink(default, audit)
        scopes = frozenset({"editor", "reviewer"})

        with pytest.raises(RuntimeError, match="refused before any statement"):
            sink.clear_flags(scopes)

        assert [inspect.getcoroutinestate(step) for step in default.made] == [inspect.CORO_CLOSED]

    def test_an_elevable_scope_of_two_stores_is_refused_without_a_product(self) -> None:
        default, audit = _Store(_store(EDITOR)), _Store(_store(EDITOR, table="e"))

        with pytest.raises(ElevableScopeInSeveralStores):
            _sink(default, audit)

    def test_it_checks_the_registered_product_against_every_store(self) -> None:
        register_authz_product(Product(CATALOG))
        default, audit = _Store(_store(EDITOR)), _Store(_store(EDITOR, table="e"))

        with pytest.raises(ElevableScopeInSeveralStores):
            _sink(default, audit)
