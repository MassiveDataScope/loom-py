"""The composition builds its one cursor codec only when a store it builds pages with cursors.

A ``signed`` codec (the default) needs keys; an application whose stores
page with no cursor (``none``) composes without them.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.composition import AppConfig, Recipe
from loom.composition.phases import build_stores
from loom.core.config import ConfigContext, ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.model import BaseModel, ColumnField
from loom.core.stores import SERVER_PROFILE, BackendRuntime
from loom.core.stores.catalog import DEFAULT_STORE, StoreKey, load_catalog
from loom.core.stores.none import NoneBackend
from loom.core.stores.repository import CursorCodec
from loom.stores.sqlalchemy.backend import SQLAlchemyBackend
from tests.helpers.composition import composition_source

_SQLITE = {"default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///:memory:"}}


class AcmeCursorNote(BaseModel):
    __tablename__ = "cursor_codec_notes"

    id: int = ColumnField(primary_key=True, autoincrement=True)


def _build(raw: dict[str, Any], *, connection_only: bool = False) -> None:
    source = composition_source(ConfigContext.from_dict(raw), app=AppConfig(name="notes"))
    build_stores(source, _recipe(connection_only=connection_only))


def _recipe(*, connection_only: bool = False) -> Recipe:
    return Recipe(
        profile=SERVER_PROFILE,
        discovered=DiscoveryResult(models=(AcmeCursorNote,), use_cases=(), interfaces=()),
        connection_only=connection_only,
    )


class TestStoreCursorCodec:
    @pytest.fixture
    def runtimes(self, monkeypatch: pytest.MonkeyPatch) -> list[BackendRuntime]:
        """Every runtime a ``none`` or SQLAlchemy store is built with, in order."""
        seen: list[BackendRuntime] = []
        for backend in (NoneBackend, SQLAlchemyBackend):
            build = backend.build

            def recorded(self: Any, ctx: Any, models: Any, runtime: Any, build: Any = build) -> Any:
                seen.append(runtime)
                return build(self, ctx, models, runtime)

            monkeypatch.setattr(backend, "build", recorded)
        return seen

    @pytest.mark.parametrize(
        "raw",
        [{}, {"stores": {"default": {"backend": "none"}}}],
        ids=["no-stores", "declared-none"],
    )
    def test_stores_paging_with_no_cursor_need_no_signing_key(
        self, raw: dict[str, Any], runtimes: list[BackendRuntime]
    ) -> None:
        _build(raw)

        assert [runtime.cursor_codec for runtime in runtimes] == [None]

    def test_a_store_paging_with_cursors_shares_one_codec(
        self, runtimes: list[BackendRuntime]
    ) -> None:
        _build({"stores": _SQLITE, "persistence": {"cursors": {"signing": "ephemeral"}}})

        assert [type(runtime.cursor_codec) for runtime in runtimes] == [CursorCodec]

    def test_a_store_paging_with_cursors_still_needs_a_signing_key(self) -> None:
        with pytest.raises(ConfigError, match="cursor signing needs a key"):
            _build({"stores": _SQLITE, "persistence": {"cursors": {"signing": "signed"}}})

    def test_an_implicit_default_store_paging_with_cursors_builds_the_codec(self) -> None:
        ctx = ConfigContext.from_dict({"persistence": {"cursors": {"signing": "ephemeral"}}})

        codec = _recipe().cursor_codec(ctx, load_catalog(ctx).with_default("sqlalchemy"))

        assert isinstance(codec, CursorCodec)

    def test_an_implicit_default_store_paging_with_no_cursor_builds_no_codec(self) -> None:
        ctx = ConfigContext.from_dict({})

        assert _recipe().cursor_codec(ctx, load_catalog(ctx).with_default("none")) is None

    def test_a_connection_only_composition_builds_no_codec(
        self, runtimes: list[BackendRuntime]
    ) -> None:
        _build({"stores": _SQLITE}, connection_only=True)

        assert [runtime.cursor_codec for runtime in runtimes] == [None]


class TestStoreCatalogCursors:
    @pytest.mark.parametrize(
        ("backend", "pages"), [("none", False), ("sqlalchemy", True)], ids=["none", "sqlalchemy"]
    )
    def test_the_catalog_pages_with_cursors_when_a_store_does(
        self, backend: str, pages: bool
    ) -> None:
        ctx = ConfigContext.from_dict({"stores": {"default": {"backend": backend}}})

        assert load_catalog(ctx).pages_with_cursors is pages

    def test_a_declared_default_store_keeps_the_catalog(self) -> None:
        catalog = load_catalog(ConfigContext.from_dict({"stores": _SQLITE}))

        assert catalog.with_default("none").specs is catalog.specs

    def test_an_undeclared_default_store_joins_with_the_given_backend(self) -> None:
        raw = {"stores": {"ledger": {"backend": "none"}}}
        catalog = load_catalog(ConfigContext.from_dict(raw))

        specs = catalog.with_default("sqlalchemy").specs

        assert list(specs) == [DEFAULT_STORE, StoreKey("ledger")]
        assert specs[DEFAULT_STORE].backend == "sqlalchemy"
        assert specs[DEFAULT_STORE].capabilities.pages_with_cursors
