"""The composition builds its one cursor codec only when ``stores.settings.cursors`` is configured.

A ``signed`` codec (the default) needs keys; an application with no cursor
settings composes without them and issues no cursor.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.composition import AppConfig, Recipe
from loom.composition.phases import build_stores
from loom.connections.aws import AwsClients
from loom.core.config import ConfigContext, ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.model import BaseModel, ColumnField
from loom.core.stores import SERVER_PROFILE, StoreRuntime
from loom.core.stores.none import NoneBackend
from loom.core.stores.repository import CursorCodec
from loom.stores.sqlalchemy.plugin import SQLAlchemyBackend
from tests.helpers.composition import composition_project

_SQLITE = {"default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///:memory:"}}


class AcmeCursorNote(BaseModel):
    __tablename__ = "cursor_codec_notes"

    id: int = ColumnField(primary_key=True, autoincrement=True)


def _build(raw: dict[str, Any], *, connection_only: bool = False) -> None:
    project = composition_project(ConfigContext.from_dict(raw), app=AppConfig(name="notes"))
    build_stores(project, _recipe(connection_only=connection_only), AwsClients())


def _recipe(*, connection_only: bool = False) -> Recipe:
    return Recipe(
        profile=SERVER_PROFILE,
        discovered=DiscoveryResult(models=(AcmeCursorNote,), use_cases=()),
        connection_only=connection_only,
    )


class TestStoreCursorCodec:
    @pytest.fixture
    def runtimes(self, monkeypatch: pytest.MonkeyPatch) -> list[StoreRuntime]:
        """Every runtime a ``none`` or SQLAlchemy store is built with, in order."""
        seen: list[StoreRuntime] = []
        for backend in (NoneBackend, SQLAlchemyBackend):
            build = backend.build

            def recorded(self: Any, ctx: Any, models: Any, runtime: Any, build: Any = build) -> Any:
                seen.append(runtime)
                return build(self, ctx, models, runtime)

            monkeypatch.setattr(backend, "build", recorded)
        return seen

    @pytest.mark.parametrize(
        "raw",
        [{}, {"stores": _SQLITE}, {"stores": {"default": {"backend": "none"}}}],
        ids=["no-stores", "sqlalchemy-without-keys", "declared-none"],
    )
    def test_without_cursor_settings_the_codec_is_disabled(
        self, raw: dict[str, Any], runtimes: list[StoreRuntime]
    ) -> None:
        _build(raw)

        assert [runtime.cursor_codec for runtime in runtimes] == [CursorCodec.disabled()]

    def test_configured_cursor_settings_share_one_codec(self, runtimes: list[StoreRuntime]) -> None:
        _build({"stores": {**_SQLITE, "settings": {"cursors": {"signing": "ephemeral"}}}})

        assert [type(runtime.cursor_codec) for runtime in runtimes] == [CursorCodec]

    def test_cursor_settings_without_a_signing_key_fail_naming_the_section(self) -> None:
        with pytest.raises(ConfigError, match=r"stores\.settings\.cursors\.current_key_id"):
            _build({"stores": {**_SQLITE, "settings": {"cursors": {"signing": "signed"}}}})

    def test_an_absent_section_builds_the_disabled_codec(self) -> None:
        codec = _recipe().build_cursor_codec(ConfigContext.from_dict({}))

        assert codec is CursorCodec.disabled()

    def test_a_configured_section_builds_the_codec(self) -> None:
        ctx = ConfigContext.from_dict(
            {"stores": {"settings": {"cursors": {"signing": "ephemeral"}}}}
        )

        assert isinstance(_recipe().build_cursor_codec(ctx), CursorCodec)

    def test_a_connection_only_composition_gets_the_disabled_codec(
        self, runtimes: list[StoreRuntime]
    ) -> None:
        _build({"stores": _SQLITE}, connection_only=True)

        assert [runtime.cursor_codec for runtime in runtimes] == [CursorCodec.disabled()]

    def test_a_connection_only_composition_ignores_configured_cursor_settings(
        self, runtimes: list[StoreRuntime]
    ) -> None:
        _build(
            {"stores": {**_SQLITE, "settings": {"cursors": {"signing": "ephemeral"}}}},
            connection_only=True,
        )

        assert [runtime.cursor_codec for runtime in runtimes] == [CursorCodec.disabled()]
