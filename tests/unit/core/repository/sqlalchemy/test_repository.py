from __future__ import annotations

from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import msgspec
import pytest

from loom.core.repository.abc import (
    CursorCodec,
    PageParams,
    PageResult,
    PaginationMode,
    QuerySpec,
    SortSpec,
)
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy, with_session_scope
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class TestRepositorySessionScope:
    @pytest.mark.asyncio
    async def test_scope_commits_when_session_has_changes(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager),
            model=dummy_model,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        mock_session.new = {"pending"}

        async with repo._session_scope(None) as session:
            assert session is mock_session

        mock_session.commit.assert_awaited_once()
        mock_session.rollback.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_scope_commits_when_session_is_read_only(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager),
            model=dummy_model,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        mock_session.new = set()
        mock_session.dirty = set()
        mock_session.deleted = set()

        async with repo._session_scope(None) as session:
            assert session is mock_session

        mock_session.commit.assert_awaited_once()
        mock_session.rollback.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_scope_rolls_back_on_error(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager),
            model=dummy_model,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )

        scope = repo._session_scope(None)
        with pytest.raises(RuntimeError, match="boom"):
            async with scope:
                raise RuntimeError("boom")

        mock_session.rollback.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_scope_reuses_explicit_session_without_commit_or_rollback(
        self,
        mock_session_manager: Any,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager),
            model=dummy_model,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        explicit_session = AsyncMock()

        async with repo._session_scope(explicit_session):
            pass

        explicit_session.commit.assert_not_awaited()
        explicit_session.rollback.assert_not_awaited()


class TestWithSessionScopeDecorator:
    @pytest.mark.asyncio
    async def test_decorator_opens_scoped_session_and_commits(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
        dummy_model: type,
    ) -> None:
        class _CustomRepo(RepositorySQLAlchemy[msgspec.Struct, int]):
            @with_session_scope
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
        mock_session.flush.assert_awaited_once()
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_decorator_uses_explicit_session_without_auto_commit(
        self,
        mock_session_manager: Any,
        dummy_model: type,
    ) -> None:
        class _CustomRepo(RepositorySQLAlchemy[msgspec.Struct, int]):
            @with_session_scope
            async def custom_operation(self, session: Any, value: int) -> int:
                await session.flush()
                return value + 1

        repo = _CustomRepo(
            session_manager=cast(SessionManager, mock_session_manager),
            model=dummy_model,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        explicit_session = AsyncMock()

        result = await repo.custom_operation(10, session=explicit_session)

        assert result == 11
        explicit_session.flush.assert_awaited_once()
        explicit_session.commit.assert_not_awaited()
        explicit_session.rollback.assert_not_awaited()


class TestRepositoryCoreReadFastPath:
    @pytest.mark.asyncio
    async def test_get_by_id_uses_core_row_mapping_path(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager),
            model=dummy_model,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        mock_session.get = AsyncMock(side_effect=AssertionError("ORM get should not be used"))

        mappings = Mock()
        mappings.first.return_value = {"id": 7}
        execute_result = Mock()
        execute_result.mappings.return_value = mappings
        mock_session.execute = AsyncMock(return_value=execute_result)

        loaded = await repo.get_by_id(7)

        assert loaded is not None
        assert getattr(loaded, "id", None) == 7
        mock_session.execute.assert_awaited_once()
        mock_session.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_list_paginated_uses_core_row_mapping_path(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager),
            model=dummy_model,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )

        item_mappings = Mock()
        item_mappings.__iter__ = Mock(
            return_value=iter(
                [
                    {"id": 1, "__loom_total_count": 2},
                    {"id": 2, "__loom_total_count": 2},
                ]
            )
        )
        item_result = Mock()
        item_result.mappings.return_value = item_mappings
        mock_session.execute = AsyncMock(return_value=item_result)

        page = await repo.list_paginated(PageParams(page=1, limit=10))

        assert [getattr(item, "id", None) for item in page.items] == [1, 2]
        assert page.total_count == 2
        assert mock_session.execute.await_count == 1

    @pytest.mark.asyncio
    async def test_list_with_query_uses_core_read_path(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
        dummy_model: type,
    ) -> None:
        repo: RepositorySQLAlchemy[msgspec.Struct, int] = RepositorySQLAlchemy(
            session_manager=cast(SessionManager, mock_session_manager),
            model=dummy_model,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        mappings = Mock()
        mappings.__iter__ = Mock(return_value=iter([{"id": 1, "__loom_total_count": 1}]))
        execute_result = Mock()
        execute_result.mappings.return_value = mappings
        mock_session.execute = AsyncMock(return_value=execute_result)

        page = await repo.list_with_query(
            QuerySpec(
                sort=(SortSpec(field="id", direction="ASC"),),
                pagination=PaginationMode.OFFSET,
                limit=10,
                page=1,
            )
        )

        assert isinstance(page, PageResult)
        assert [getattr(item, "id", None) for item in page.items] == [1]
        assert page.total_count == 1
        assert mock_session.execute.await_count == 1
