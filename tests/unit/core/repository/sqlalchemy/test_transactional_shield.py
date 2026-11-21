"""``@transactional``'s drain shields only the priority lane, not the plain one.

Mirrors ``tests/unit/core/engine/test_post_commit_shield.py`` for the
decorator's own drain call (``transactional.py:181``): the same hazard the
executor has exists here too, and the same fix applies — inside
``PostCommitChannel.drain`` itself, not by shielding the whole call.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loom.core.engine.post_commit import PostCommitChannel, active_channel
from loom.stores.sqlalchemy.transactional import transactional

from .conftest import MockSessionManager


class _SlowPriorityOwner:
    """A ``@transactional`` owner whose body queues a priority action that blocks on a gate."""

    def __init__(self, session_manager: MockSessionManager, gate: asyncio.Event) -> None:
        self.session_manager = session_manager
        self._gate = gate
        self.log: list[str] = []

    @transactional
    async def execute(self) -> str:
        channel: PostCommitChannel | None = active_channel()
        assert channel is not None

        async def _slow() -> None:
            self.log.append("priority.started")
            await self._gate.wait()
            self.log.append("priority.finished")

        channel.enqueue_priority(_slow)
        return "ok"


class _SlowDispatchOwner:
    """A ``@transactional`` owner whose body queues a plain action that blocks on a gate."""

    def __init__(self, session_manager: MockSessionManager, gate: asyncio.Event) -> None:
        self.session_manager = session_manager
        self._gate = gate
        self.log: list[str] = []

    @transactional
    async def execute(self) -> str:
        channel: PostCommitChannel | None = active_channel()
        assert channel is not None

        async def _slow() -> None:
            self.log.append("dispatch.started")
            await self._gate.wait()
            self.log.append("dispatch.finished")

        channel.enqueue(_slow)
        return "ok"


class TestTransactionalShield:
    @pytest.mark.asyncio
    async def test_a_cancellation_after_commit_does_not_abort_the_shielded_priority_lane(
        self,
        mock_session_manager: MockSessionManager,
    ) -> None:
        gate = asyncio.Event()
        owner = _SlowPriorityOwner(mock_session_manager, gate)

        async def _run() -> str:
            return await owner.execute()

        task: asyncio.Task[Any] = asyncio.create_task(_run())
        while "priority.started" not in owner.log:
            await asyncio.sleep(0)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The caller gave up, but the shielded priority lane kept running in
        # the background: releasing the gate lets it finish.
        gate.set()
        await asyncio.sleep(0.01)

        assert "priority.finished" in owner.log

    @pytest.mark.asyncio
    async def test_a_cancellation_after_commit_still_aborts_a_plain_action_in_flight(
        self,
        mock_session_manager: MockSessionManager,
    ) -> None:
        """The property the priority lane's shield must not widen: the plain lane stays
        cancellable.
        """
        gate = asyncio.Event()
        owner = _SlowDispatchOwner(mock_session_manager, gate)

        async def _run() -> str:
            return await owner.execute()

        task: asyncio.Task[Any] = asyncio.create_task(_run())
        while "dispatch.started" not in owner.log:
            await asyncio.sleep(0)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # Unlike the priority lane, the plain lane really was interrupted.
        gate.set()
        await asyncio.sleep(0.01)

        assert "dispatch.finished" not in owner.log
