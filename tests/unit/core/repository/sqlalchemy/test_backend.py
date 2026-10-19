"""``SQLAlchemyBackend``: the readiness probe over the shared engine."""

from __future__ import annotations

import logging

import pytest

import loom.core.repository.sqlalchemy.backend as backend_module
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.persistence import PersistenceWiring
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend

_UNREACHABLE_URL = "sqlite+aiosqlite:////nonexistent/loom-readiness/store.db"


def _wiring(url: str, cursors: dict[str, str] | None = None) -> PersistenceWiring:
    ctx = ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "database": {"url": url},
            "persistence": {"cursors": {"signing": "ephemeral"} if cursors is None else cursors},
        }
    )
    return SQLAlchemyBackend().build(ctx, ())


def test_a_missing_cursor_key_fails_the_build_naming_the_opt_out() -> None:
    with pytest.raises(ConfigError, match=r"persistence\.cursors\.signing: ephemeral"):
        _wiring("sqlite+aiosqlite:///", cursors={})


def test_an_unsigned_build_warns_at_startup(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        _wiring("sqlite+aiosqlite:///", cursors={"signing": "unsigned"})

    assert "unsigned" in caplog.text


def test_the_wiring_registers_its_cursor_codec_for_direct_users() -> None:
    container = LoomContainer()

    _wiring("sqlite+aiosqlite:///").repo_registration_module(container)

    assert isinstance(container.resolve(CursorCodec), CursorCodec)


@pytest.mark.asyncio
async def test_readiness_is_true_when_the_database_answers() -> None:
    wiring = _wiring("sqlite+aiosqlite:///")

    assert wiring.readiness is not None
    async with wiring.lifespan_init():
        assert await wiring.readiness() is True


@pytest.mark.asyncio
async def test_readiness_is_false_when_the_database_is_unreachable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    wiring = _wiring(_UNREACHABLE_URL)

    assert wiring.readiness is not None
    with caplog.at_level(logging.WARNING, logger=backend_module.__name__):
        assert await wiring.readiness() is False

    assert "readiness" in caplog.text
