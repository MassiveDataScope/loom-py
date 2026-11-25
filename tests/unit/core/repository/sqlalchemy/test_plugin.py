"""``SQLAlchemyStorePlugin``: the readiness probe over the shared engine."""

from __future__ import annotations

import logging

import pytest

import loom.stores.sqlalchemy.plugin as backend_module
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.stores import StoreWiring
from loom.core.stores.profile import SERVER_PROFILE
from loom.core.stores.repository import CursorCodec
from loom.stores.sqlalchemy.plugin import SQLAlchemyStorePlugin
from tests.helpers.loggers import CapturedLog
from tests.helpers.persistence import build_backend

_UNREACHABLE_URL = "sqlite+aiosqlite:////nonexistent/loom-readiness/store.db"


def _wiring(url: str, cursors: dict[str, str] | None = None) -> StoreWiring:
    ctx = ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "stores": {
                "default": {"backend": "sqlalchemy", "url": url},
                "settings": {"cursors": {"signing": "ephemeral"} if cursors is None else cursors},
            },
        }
    )
    return build_backend(SQLAlchemyStorePlugin(), ctx, (), SERVER_PROFILE)


class TestSqlAlchemyStorePlugin:
    def test_a_missing_cursor_key_fails_the_build_naming_the_opt_out(self) -> None:
        with pytest.raises(ConfigError, match=r"stores\.settings\.cursors\.signing: ephemeral"):
            _wiring("sqlite+aiosqlite:///", cursors={})

    def test_an_unsigned_build_warns_at_startup(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            _wiring("sqlite+aiosqlite:///", cursors={"signing": "unsigned"})

        assert "unsigned" in CapturedLog(caplog).laid_out_text

    def test_the_wiring_registers_its_cursor_codec_for_direct_users(self) -> None:
        container = LoomContainer()

        _wiring("sqlite+aiosqlite:///").repo_registration_module(container)

        assert isinstance(container.resolve(CursorCodec), CursorCodec)

    @pytest.mark.asyncio
    async def test_readiness_is_true_when_the_database_answers(self) -> None:
        wiring = _wiring("sqlite+aiosqlite:///")

        assert wiring.readiness is not None
        async with wiring.lifespan_init():
            assert await wiring.readiness() is True

    @pytest.mark.asyncio
    async def test_readiness_is_false_when_the_database_is_unreachable(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        wiring = _wiring(_UNREACHABLE_URL)

        assert wiring.readiness is not None
        with caplog.at_level(logging.WARNING, logger=backend_module.__name__):
            assert await wiring.readiness() is False

        assert "readiness" in CapturedLog(caplog).laid_out_text
