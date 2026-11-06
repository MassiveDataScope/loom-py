"""``@transactional`` runs its post-commit hooks through the shared channel."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.core.engine.post_commit import (
    PostCommitChannel,
    active_channel,
    bind_channel,
    reset_channel,
)
from loom.core.errors import PostCommitError
from loom.core.repository.mutation import MutationEvent
from loom.core.uow.scope import current_scope

from .conftest import OuterService, PostCommitService, ServiceWithMixedDependencies


class TestOwnedChannel:
    async def test_hooks_run_in_order_after_commit_and_session_close(
        self, service: PostCommitService, log: list[str]
    ) -> None:
        result = await service.execute()

        assert result == "ok"
        assert log == ["body", "commit", "session_closed", "owner", "dep_a", "dep_b"]

    async def test_hooks_run_outside_the_invocation(self, service: PostCommitService) -> None:
        seen: list[Any] = []

        async def _hook(events: tuple[MutationEvent, ...]) -> None:
            seen.append(current_scope())

        service.dependency_a.on_transaction_committed = _hook  # type: ignore[method-assign]

        await service.execute()

        assert seen == [None]

    async def test_rollback_discards_hooks(
        self, service: PostCommitService, recording_session: MagicMock, log: list[str]
    ) -> None:
        with pytest.raises(RuntimeError, match="boom"):
            await service.fail()

        recording_session.rollback.assert_awaited_once()
        assert log == ["session_closed"]
        assert active_channel() is None

    async def test_failing_hook_raises_post_commit_error_after_the_others_ran(
        self, service: PostCommitService, log: list[str]
    ) -> None:
        service.owner_error = ValueError("hook failed")

        with pytest.raises(PostCommitError) as excinfo:
            await service.execute()

        assert excinfo.value.committed is True
        assert excinfo.value.failures == (service.owner_error,)
        assert log[-3:] == ["owner", "dep_a", "dep_b"]

    async def test_channel_unbound_after_success(self, service: PostCommitService) -> None:
        await service.execute()

        assert active_channel() is None

    async def test_nested_call_under_active_session_enqueues_nothing_extra(
        self, outer_service: OuterService, recording_session: MagicMock, log: list[str]
    ) -> None:
        await outer_service.execute()

        recording_session.commit.assert_awaited_once()
        # The owner walk is one level deep: ``inner`` is a dependency of
        # ``outer`` and runs once; it did not bind a channel of its own.
        assert log == ["body", "commit", "session_closed", "outer", "owner"]


class TestBoundChannel:
    async def test_owns_its_channel_even_when_one_is_bound(
        self, service: PostCommitService, log: list[str]
    ) -> None:
        outer = PostCommitChannel()
        outer.enqueue(lambda: log.append("outer_action"))
        token = bind_channel(outer)
        try:
            await service.execute()

            assert log == ["body", "commit", "session_closed", "owner", "dep_a", "dep_b"]
            assert active_channel() is outer

            await outer.drain(committed=True)
        finally:
            reset_channel(token)

        assert log[-1] == "outer_action"


class TestDependencyWalk:
    """Only real post-commit destinations are notified, and each exactly once."""

    async def test_only_post_commit_destinations_are_notified(
        self, mixed_dependency_service: ServiceWithMixedDependencies, log: list[str]
    ) -> None:
        await mixed_dependency_service.execute()

        # The plain collaborator (no hook), the plain value ``retries`` and the
        # back-reference to the service itself add nothing to the walk.
        assert log == ["commit", "session_closed", "owner", "repository"]
        assert mixed_dependency_service.repository.events == (
            MutationEvent(entity="incident", op="update", ids=(7,)),
        )
