from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import msgspec
import pytest

from loom.core.stores.repository import CursorCodec
from loom.stores.sqlalchemy.repository import SQLAlchemyRepository, with_session_scope
from loom.stores.sqlalchemy.session_manager import SessionManager

from ...conftest import RecordingSession


def _session_of(session_manager: Any) -> RecordingSession:
    """A session double bound to ``session_manager``'s engine."""
    return RecordingSession(bind=SimpleNamespace(engine=session_manager.engine.sync_engine))


class TestRepositorySessionScope:
    @pytest.mark.asyncio
    async def test_scope_reuses_explicit_session_without_commit_or_rollback(
        self,
        mock_session_manager: Any,
        dummy_model: type,
    ) -> None:
        repo: SQLAlchemyRepository[msgspec.Struct, int] = SQLAlchemyRepository(
            session_manager=cast(SessionManager, mock_session_manager),
            model=dummy_model,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        explicit_session = _session_of(mock_session_manager)

        async with repo._session_scope(explicit_session, write=False):
            pass

        assert explicit_session.calls == []


class TestWithSessionScopeDecorator:
    @pytest.mark.asyncio
    async def test_decorator_opens_scoped_session_and_commits(
        self,
        mock_session_manager: Any,
        session_double: RecordingSession,
        dummy_model: type,
    ) -> None:
        class _CustomRepo(SQLAlchemyRepository[msgspec.Struct, int]):
            @with_session_scope(write=True)
            async def custom_operation(self, session: Any, value: int) -> int:
                await session.flush()
                return value + 1

        repo = _CustomRepo(
            session_manager=cast(SessionManager, mock_session_manager),
            model=dummy_model,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )

        result = await repo.custom_operation(41)

        assert result == 42
        assert session_double.calls == ["flush", "commit"]

    @pytest.mark.asyncio
    async def test_decorator_uses_explicit_session_without_auto_commit(
        self,
        mock_session_manager: Any,
        dummy_model: type,
    ) -> None:
        class _CustomRepo(SQLAlchemyRepository[msgspec.Struct, int]):
            @with_session_scope(write=True)
            async def custom_operation(self, session: Any, value: int) -> int:
                await session.flush()
                return value + 1

        repo = _CustomRepo(
            session_manager=cast(SessionManager, mock_session_manager),
            model=dummy_model,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        explicit_session = _session_of(mock_session_manager)

        result = await repo.custom_operation(10, session=explicit_session)

        assert result == 11
        assert explicit_session.calls == ["flush"]
