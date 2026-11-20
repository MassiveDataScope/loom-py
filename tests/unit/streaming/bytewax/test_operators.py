"""Tests for Bytewax resource lifecycle helpers."""

from __future__ import annotations

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.async_bridge import AsyncBridge  # noqa: E402
from loom.core.model import LoomStruct  # noqa: E402
from loom.streaming import IntoTopic, Process, ResourceScope, With, WithAsync  # noqa: E402
from loom.streaming.bytewax._operators import (  # noqa: E402
    AsyncResourceLifecycle,
    SyncResourceLifecycle,
    lifecycle_for,
)
from loom.streaming.nodes._with import ContextSupplier  # noqa: E402

pytestmark = pytest.mark.bytewax


class _FakeSyncCM:
    def __init__(self, *, fail_on_enter: bool = False) -> None:
        self.fail_on_enter = fail_on_enter
        self.entered = 0
        self.exited = 0

    def __enter__(self) -> _FakeSyncCM:
        self.entered += 1
        if self.fail_on_enter:
            raise RuntimeError("sync-enter-boom")
        return self

    def __exit__(self, *args: object) -> None:
        self.exited += 1


class _FakeAsyncCM:
    def __init__(self, *, fail_on_enter: bool = False) -> None:
        self.fail_on_enter = fail_on_enter
        self.entered = 0
        self.exited = 0

    async def __aenter__(self) -> _FakeAsyncCM:
        self.entered += 1
        if self.fail_on_enter:
            raise RuntimeError("async-enter-boom")
        return self

    async def __aexit__(self, *args: object) -> None:
        self.exited += 1


class AcmeOperatorPayload(LoomStruct):
    value: str = "x"


class TestSyncResourceLifecycle:
    def test_open_worker_enters_contexts_and_preserves_plain_deps(self) -> None:
        client = _FakeSyncCM()
        node: With[AcmeOperatorPayload, AcmeOperatorPayload] = With(
            process=Process(IntoTopic("out", payload=AcmeOperatorPayload)),
            client=client,
            retries=3,
        )
        lifecycle = lifecycle_for(node)

        assert isinstance(lifecycle, SyncResourceLifecycle)
        resources = lifecycle.open_worker()

        assert resources["client"] is client
        assert resources["retries"] == 3
        assert client.entered == 1

        lifecycle.shutdown()
        assert client.exited == 1

    def test_open_batch_rolls_back_entered_contexts_on_failure(self) -> None:
        first = _FakeSyncCM()
        second = _FakeSyncCM(fail_on_enter=True)
        node: With[AcmeOperatorPayload, AcmeOperatorPayload] = With(
            process=Process(IntoTopic("out", payload=AcmeOperatorPayload)),
            scope=ResourceScope.BATCH,
            first=first,
            second=second,
        )
        lifecycle = lifecycle_for(node)

        with pytest.raises(RuntimeError, match="sync-enter-boom"):
            lifecycle.open_batch()

        assert first.entered == 1
        assert first.exited == 1

    def test_context_supplier_creates_fresh_context_per_batch(self) -> None:
        created: list[_FakeSyncCM] = []

        def _factory() -> _FakeSyncCM:
            cm = _FakeSyncCM()
            created.append(cm)
            return cm

        node: With[AcmeOperatorPayload, AcmeOperatorPayload] = With(
            process=Process(IntoTopic("out", payload=AcmeOperatorPayload)),
            scope=ResourceScope.BATCH,
            client=ContextSupplier(_factory),
        )
        lifecycle = lifecycle_for(node)

        lifecycle.open_batch()
        lifecycle.close_batch()
        lifecycle.open_batch()
        lifecycle.close_batch()

        assert len(created) == 2
        assert created[0].entered == 1
        assert created[1].entered == 1


class TestAsyncResourceLifecycle:
    def test_open_worker_enters_async_contexts_and_preserves_plain_deps(self) -> None:
        client = _FakeAsyncCM()
        node: WithAsync[AcmeOperatorPayload, AcmeOperatorPayload] = WithAsync(
            process=Process(IntoTopic("out", payload=AcmeOperatorPayload)),
            client=client,
            validator="plain",
        )
        bridge = AsyncBridge()
        try:
            lifecycle = lifecycle_for(node, bridge=bridge)

            assert isinstance(lifecycle, AsyncResourceLifecycle)
            resources = lifecycle.open_worker()

            assert resources["client"] is client
            assert resources["validator"] == "plain"
            assert client.entered == 1

            lifecycle.shutdown()
            assert client.exited == 1
        finally:
            bridge.shutdown()

    def test_open_batch_rolls_back_async_contexts_on_failure(self) -> None:
        first = _FakeAsyncCM()
        second = _FakeAsyncCM(fail_on_enter=True)
        node: WithAsync[AcmeOperatorPayload, AcmeOperatorPayload] = WithAsync(
            process=Process(IntoTopic("out", payload=AcmeOperatorPayload)),
            scope=ResourceScope.BATCH,
            first=first,
            second=second,
        )
        bridge = AsyncBridge()
        try:
            lifecycle = lifecycle_for(node, bridge=bridge)

            with pytest.raises(RuntimeError, match="async-enter-boom"):
                lifecycle.open_batch()

            assert first.entered == 1
            assert first.exited == 1
        finally:
            bridge.shutdown()

    def test_lifecycle_for_requires_bridge_for_async_nodes(self) -> None:
        node: WithAsync[AcmeOperatorPayload, AcmeOperatorPayload] = WithAsync(
            process=Process(IntoTopic("out", payload=AcmeOperatorPayload))
        )

        with pytest.raises(ValueError, match="AsyncBridge"):
            lifecycle_for(node)
