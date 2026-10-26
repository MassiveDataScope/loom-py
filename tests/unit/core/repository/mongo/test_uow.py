"""``MongoUnitOfWork``: no-op by default, session + transaction when configured."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

import pytest
from pymongo import AsyncMongoClient

import loom.core.repository.mongo.uow as uow_module
from loom.core.repository.abc import CursorCodec
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import MongoUnitOfWorkFactory, active_session
from loom.core.transaction import in_atomic_transaction

from ._fake import FakeMongoClient
from .conftest import Article
from .test_repository import ArticleCreate

pytestmark = pytest.mark.asyncio


class _WriteFailed(RuntimeError):
    pass


def _transactional(client: FakeMongoClient) -> MongoUnitOfWorkFactory:
    # The fake stands in for the nominal driver client.
    return MongoUnitOfWorkFactory.transactional(cast(AsyncMongoClient[Any], client))


def _repository(client: FakeMongoClient) -> RepositoryMongo[Article, str]:
    return RepositoryMongo(
        Article,
        client.collection("articles"),
        session_provider=active_session,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )


class TestWithoutTransactions:
    async def test_opens_no_session(self) -> None:
        client = FakeMongoClient()
        factory = MongoUnitOfWorkFactory.without_transactions()

        async with factory.create():
            assert active_session() is None

        assert client.sessions == []
        assert client.committed == 0

    async def test_leaves_the_atomic_transaction_signal_closed(self) -> None:
        """F03: writes autocommit here, so the cache bump must stay inline."""
        factory = MongoUnitOfWorkFactory.without_transactions()

        async with factory.create():
            assert in_atomic_transaction() is False

        assert in_atomic_transaction() is False

    async def test_repository_writes_without_a_session(self) -> None:
        client = FakeMongoClient()
        factory = MongoUnitOfWorkFactory.without_transactions()
        repository = _repository(client)

        async with factory.create():
            await repository.create(ArticleCreate(slug="a", title="A"))

        assert client.collection("articles").last_session is None
        assert "a" in client.collection("articles").documents

    async def test_exception_propagates_and_nothing_is_aborted(self) -> None:
        client = FakeMongoClient()
        factory = MongoUnitOfWorkFactory.without_transactions()
        uow = factory.create()

        with pytest.raises(_WriteFailed):
            async with uow:
                raise _WriteFailed()

        assert client.aborted == 0

    async def test_leaves_the_active_session_untouched(self) -> None:
        """A no-op unit of work inside a transactional one does not hide its session."""
        client = FakeMongoClient()

        async with _transactional(client).create():
            outer = active_session()
            async with MongoUnitOfWorkFactory.without_transactions().create():
                assert active_session() is outer
            assert active_session() is outer


class TestTransactional:
    async def test_commits_on_clean_exit_and_ends_the_session(self) -> None:
        client = FakeMongoClient()

        async with _transactional(client).create():
            session = client.sessions[0]
            assert active_session() is cast(Any, session)
            assert session.in_transaction is True

        assert client.committed == 1
        assert client.aborted == 0
        assert session.ended is True
        assert active_session() is None

    async def test_opens_the_atomic_transaction_signal_and_closes_it_after(self) -> None:
        """F03: a write here is not durable until commit, so the bump must defer."""
        client = FakeMongoClient()

        assert in_atomic_transaction() is False
        async with _transactional(client).create():
            assert in_atomic_transaction() is True

        assert in_atomic_transaction() is False

    async def test_aborts_on_exception_and_ends_the_session(self) -> None:
        client = FakeMongoClient()
        uow = _transactional(client).create()

        with pytest.raises(_WriteFailed):
            async with uow:
                raise _WriteFailed()

        assert client.aborted == 1
        assert client.committed == 0
        assert client.sessions[0].ended is True
        assert active_session() is None

    async def test_repository_call_inside_the_unit_of_work_uses_its_session(self) -> None:
        client = FakeMongoClient()
        repository = _repository(client)

        async with _transactional(client).create():
            await repository.create(ArticleCreate(slug="a", title="A"))

        assert client.collection("articles").last_session is client.sessions[0]

    async def test_second_write_failing_hides_the_first(self) -> None:
        """US2 s4: two writes, the second fails, neither is visible."""
        client = FakeMongoClient()
        repository = _repository(client)
        uow = _transactional(client).create()
        payload = ArticleCreate(slug="a", title="A")
        error = _WriteFailed()

        with pytest.raises(_WriteFailed):
            async with uow:
                await repository.create(payload)
                raise error

        assert client.aborted == 1
        assert await repository.get_by_id("a") is None

    async def test_contextvar_is_reset_even_when_abort_fails(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        client = FakeMongoClient()
        client.abort_error = ConnectionError("replica set gone")
        uow = _transactional(client).create()

        with (
            caplog.at_level(logging.ERROR, logger=uow_module.__name__),
            pytest.raises(_WriteFailed),
        ):
            async with uow:
                raise _WriteFailed()

        assert active_session() is None
        assert client.sessions[0].ended is True
        assert "UoWRollbackFailed" in caplog.text

    async def test_session_is_ended_when_start_transaction_fails(self) -> None:
        client = FakeMongoClient()
        client.start_transaction_error = ConnectionError("not a replica set")
        uow = _transactional(client).create()

        with pytest.raises(ConnectionError, match="replica set"):
            async with uow:
                raise AssertionError("body must not run")

        assert client.sessions[0].ended is True
        assert active_session() is None

    async def test_end_session_failure_keeps_the_use_case_exception(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        client = FakeMongoClient()
        client.end_error = ConnectionError("session gone")
        uow = _transactional(client).create()

        with (
            caplog.at_level(logging.ERROR, logger=uow_module.__name__),
            pytest.raises(_WriteFailed),
        ):
            async with uow:
                raise _WriteFailed()

        assert active_session() is None
        assert "UoWCloseFailed" in caplog.text

    async def test_commit_failure_ends_the_session_without_a_second_abort(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """US1 s2: pymongo marks the transaction itself; no ``InvalidOperation`` follows."""
        client = FakeMongoClient()
        client.commit_error = ConnectionError("commit lost")
        uow = _transactional(client).create()

        with (
            caplog.at_level(logging.ERROR, logger=uow_module.__name__),
            pytest.raises(ConnectionError, match="commit lost"),
        ):
            async with uow:
                pass

        assert client.aborted == 0
        assert client.sessions[0].ended is True
        assert active_session() is None
        assert "UoWRollbackFailed" not in caplog.text
        assert "InvalidOperation" not in caplog.text

    async def test_second_cancellation_during_abort_still_aborts_and_ends(self) -> None:
        """The abort and ``end_session`` are shielded; the token is reset at once."""
        client = FakeMongoClient()
        client.abort_gate = asyncio.Event()
        uow = _transactional(client).create()

        observed: list[bool] = []

        async def run() -> None:
            try:
                async with uow:
                    await asyncio.Event().wait()
            finally:
                observed.append(active_session() is None)  # the task's own context

        task = asyncio.create_task(run())
        await asyncio.sleep(0)
        task.cancel()  # cuts the body: the abort starts and blocks on the gate
        await asyncio.sleep(0)
        task.cancel()  # arrives while the abort is in flight
        with pytest.raises(asyncio.CancelledError):
            await task
        assert observed == [True]
        assert client.aborted == 0

        client.abort_gate.set()
        await asyncio.sleep(0.01)

        assert client.aborted == 1
        assert client.sessions[0].ended is True

    async def test_begin_twice_is_refused(self) -> None:
        client = FakeMongoClient()
        uow = _transactional(client).create()
        await uow.begin()

        with pytest.raises(RuntimeError, match="begin"):
            await uow.begin()

        await uow.rollback()

    async def test_commit_before_begin_is_refused(self) -> None:
        uow = _transactional(FakeMongoClient()).create()

        with pytest.raises(RuntimeError, match="begin"):
            await uow.commit()

    async def test_units_of_work_are_independent(self) -> None:
        client = FakeMongoClient()
        factory = _transactional(client)

        async with factory.create():
            first = active_session()
        async with factory.create():
            second = active_session()

        assert first is not second
        assert len(client.sessions) == 2


class TestCallerScopedSessionProbe:
    """The capability a wrapper reads before detaching a read into its own task.

    A coalesced read runs in its own task and outlives the caller that started
    it; inside the unit of work's transaction that session is closed by the
    caller's teardown and holds writes nobody else may see.
    """

    async def test_it_reports_the_session_the_unit_of_work_publishes(self) -> None:
        client = FakeMongoClient()
        repository = _repository(client)
        factory = _transactional(client)

        assert repository.has_caller_scoped_session() is False

        async with factory.create():
            assert repository.has_caller_scoped_session() is True

        assert repository.has_caller_scoped_session() is False

    async def test_without_transactions_no_session_is_reported(self) -> None:
        client = FakeMongoClient()
        repository = _repository(client)
        factory = MongoUnitOfWorkFactory.without_transactions()

        async with factory.create():
            assert repository.has_caller_scoped_session() is False

    async def test_a_repository_with_the_default_provider_never_reports_one(self) -> None:
        client = FakeMongoClient()
        repository = RepositoryMongo(
            Article, client.collection("articles"), cursor_codec=CursorCodec(signing="ephemeral")
        )
        factory = _transactional(client)

        async with factory.create():
            assert repository.has_caller_scoped_session() is False
