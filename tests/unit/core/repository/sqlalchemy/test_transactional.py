from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import DateTime  # noqa: F401

from loom.core.repository.abc import CursorCodec
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.transactional import record_mutation, transactional

from .conftest import RepositoryWithTransactionalMethod, ServiceWithoutSessionManager


class _ServiceWithTransaction:
    def __init__(self, session_manager: Any) -> None:
        self.session_manager = session_manager
        self.on_transaction_committed = AsyncMock()

    @transactional
    async def execute(self) -> str:
        record_mutation(MutationEvent(entity="incident", op="update", ids=(1,)))
        return "ok"


class _NestedService:
    def __init__(self, session_manager: Any) -> None:
        self.session_manager = session_manager

    @transactional
    async def inner(self) -> str:
        return "inner"

    @transactional
    async def outer(self) -> str:
        return await self.inner()


class TestTransactionalDecorator:
    async def test_transaction_commit_and_post_commit_hook(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
    ) -> None:
        service = _ServiceWithTransaction(mock_session_manager)

        result = await service.execute()

        assert result == "ok"
        mock_session.commit.assert_awaited_once()
        mock_session.rollback.assert_not_awaited()
        service.on_transaction_committed.assert_awaited_once()

    async def test_nested_transaction_reuses_same_session(
        self,
        mock_session_manager: Any,
        mock_session: AsyncMock,
    ) -> None:
        service = _NestedService(mock_session_manager)

        result = await service.outer()

        assert result == "inner"
        mock_session.commit.assert_awaited_once()


class TestTransactionalOwnerContract:
    """The decorator refuses owners it cannot honour, before any session opens."""

    async def test_repository_owner_is_rejected(
        self, repository_owner: RepositoryWithTransactionalMethod, mock_session: AsyncMock
    ) -> None:
        with pytest.raises(TypeError) as excinfo:
            await repository_owner.execute()

        message = str(excinfo.value)
        assert "@transactional" in message
        assert "not repository methods" in message
        mock_session.commit.assert_not_awaited()

    async def test_owner_without_session_manager_is_rejected(
        self, owner_without_session_manager: ServiceWithoutSessionManager
    ) -> None:
        with pytest.raises(TypeError) as excinfo:
            await owner_without_session_manager.execute()

        message = str(excinfo.value)
        assert "ServiceWithoutSessionManager" in message
        assert "session_manager" in message


class _ServiceProbingItsRepository:
    """Service that asks its repository whether the session is the caller's."""

    def __init__(self, session_manager: Any, repository: Any) -> None:
        self.session_manager = session_manager
        self.repository = repository
        self.inside: bool | None = None

    @transactional
    async def execute(self) -> None:
        self.inside = self.repository.has_caller_scoped_session()


class TestCallerScopedSessionProbe:
    """The probe a wrapper reads before detaching a read into its own task."""

    async def test_it_reports_the_session_the_decorator_binds(
        self, mock_session_manager: Any, dummy_model: type
    ) -> None:
        repository = RepositorySQLAlchemy(
            mock_session_manager, dummy_model, cursor_codec=CursorCodec(signing="ephemeral")
        )
        service = _ServiceProbingItsRepository(mock_session_manager, repository)

        assert repository.has_caller_scoped_session() is False

        await service.execute()

        assert service.inside is True

    async def test_the_binding_does_not_leak_past_the_transaction(
        self, mock_session_manager: Any, dummy_model: type
    ) -> None:
        repository = RepositorySQLAlchemy(
            mock_session_manager, dummy_model, cursor_codec=CursorCodec(signing="ephemeral")
        )
        service = _ServiceProbingItsRepository(mock_session_manager, repository)

        await service.execute()

        assert repository.has_caller_scoped_session() is False
