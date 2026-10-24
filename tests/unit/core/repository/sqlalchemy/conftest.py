from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, cast
from unittest.mock import AsyncMock, MagicMock

import msgspec
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.backend.sqlalchemy import compile_all, reset_registry
from loom.core.model import BaseModel, Field, Integer
from loom.core.repository.abc import CursorCodec
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import record_mutation, transactional

from ...conftest import RecordingSessionManager


class _DummyModel(BaseModel):
    __tablename__ = "dummies"
    id: Annotated[int, Integer, Field(primary_key=True, autoincrement=True)]


class MockSessionManager:
    def __init__(self, session: Any) -> None:
        self._session = session

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        yield self._session

    def current_settings(self) -> dict[str, str]:
        return {}


@pytest.fixture(autouse=True)
def _compiled_dummy_model() -> Any:
    reset_registry()
    compile_all(_DummyModel)
    yield
    reset_registry(naming_convention=None)


@pytest.fixture
def dummy_model() -> type:
    return _DummyModel


@pytest.fixture
def mock_session() -> AsyncMock:
    session = AsyncMock()
    session.new = set()
    session.dirty = set()
    session.deleted = set()
    return session


@pytest.fixture
def mock_session_manager(mock_session: AsyncMock) -> MockSessionManager:
    return MockSessionManager(mock_session)


# ---------------------------------------------------------------------------
# ``@transactional`` harness: session manager, owners and their dependencies
# ---------------------------------------------------------------------------


class PostCommitDependency:
    """A dependency that reacts to the committed transaction."""

    def __init__(self, log: list[str], name: str) -> None:
        self._log = log
        self._name = name
        self.events: tuple[MutationEvent, ...] | None = None

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        self.events = events
        self._log.append(self._name)


class PlainCollaborator:
    """A dependency that is not a post-commit destination."""

    def __init__(self, log: list[str]) -> None:
        self._log = log

    async def refresh(self) -> None:
        self._log.append("refresh")


class PostCommitService:
    """Service that owns the transaction and two post-commit dependencies."""

    def __init__(self, session_manager: Any, log: list[str]) -> None:
        self.session_manager = session_manager
        self.log = log
        self.dependency_a = PostCommitDependency(log, "dep_a")
        self.dependency_b = PostCommitDependency(log, "dep_b")
        self.owner_error: Exception | None = None

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        self.log.append("owner")
        if self.owner_error is not None:
            raise self.owner_error

    @transactional
    async def execute(self) -> str:
        self.log.append("body")
        return "ok"

    @transactional
    async def fail(self) -> None:
        raise RuntimeError("boom")


class OuterService:
    """Service whose transactional method delegates to another transactional one."""

    def __init__(self, session_manager: Any, log: list[str]) -> None:
        self.session_manager = session_manager
        self.log = log
        self.inner = PostCommitService(session_manager, log)

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        self.log.append("outer")

    @transactional
    async def execute(self) -> str:
        return await self.inner.execute()


class ServiceWithMixedDependencies:
    """Service holding a post-commit destination among unrelated attributes."""

    def __init__(self, session_manager: Any, log: list[str]) -> None:
        self.session_manager = session_manager
        self.log = log
        self.repository = PostCommitDependency(log, "repository")
        self.collaborator = PlainCollaborator(log)
        self.retries = 3
        self.fallback = self  # a back-reference: still one hook, not two

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        self.log.append("owner")

    @transactional
    async def execute(self) -> None:
        record_mutation(MutationEvent(entity="incident", op="update", ids=(7,)))


class RepositoryWithTransactionalMethod(RepositorySQLAlchemy[msgspec.Struct, int]):
    """A repository that wrongly declares its own transaction boundary."""

    @transactional
    async def execute(self) -> str:
        return "ok"


class ServiceWithoutSessionManager:
    """A service wired without the collaborator the decorator needs."""

    @transactional
    async def execute(self) -> str:
        return "ok"


@pytest.fixture
def log() -> list[str]:
    return []


@pytest.fixture
def recording_session(session_double: MagicMock, log: list[str]) -> MagicMock:
    """A session double that records its commit in ``log``."""

    async def _commit() -> None:
        log.append("commit")

    session_double.commit.side_effect = _commit
    return session_double


@pytest.fixture
def logging_session_manager(
    recording_session: MagicMock, log: list[str]
) -> RecordingSessionManager:
    """The manager that hands out ``recording_session`` and logs its close."""
    return RecordingSessionManager(recording_session, log=log)


@pytest.fixture
def service(logging_session_manager: RecordingSessionManager, log: list[str]) -> PostCommitService:
    return PostCommitService(logging_session_manager, log)


@pytest.fixture
def outer_service(logging_session_manager: RecordingSessionManager, log: list[str]) -> OuterService:
    return OuterService(logging_session_manager, log)


@pytest.fixture
def mixed_dependency_service(
    logging_session_manager: RecordingSessionManager, log: list[str]
) -> ServiceWithMixedDependencies:
    """A service whose attributes mix a destination, a plain object and a plain value."""
    return ServiceWithMixedDependencies(logging_session_manager, log)


@pytest.fixture
def repository_owner(
    mock_session_manager: MockSessionManager, dummy_model: type
) -> RepositoryWithTransactionalMethod:
    return RepositoryWithTransactionalMethod(
        session_manager=cast(SessionManager, mock_session_manager),
        model=dummy_model,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )


@pytest.fixture
def owner_without_session_manager() -> ServiceWithoutSessionManager:
    return ServiceWithoutSessionManager()
