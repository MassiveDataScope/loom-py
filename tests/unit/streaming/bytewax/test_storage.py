"""Tests for storage sink wiring in the Bytewax adapter."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.async_bridge import AsyncBridge  # noqa: E402
from loom.core.model.redaction import Dsn  # noqa: E402
from loom.core.observability.runtime import ObservabilityRuntime  # noqa: E402
from loom.core.persistence.database import DatabaseConfig, PoolConfig  # noqa: E402
from loom.streaming.bytewax.handlers import storage as _storage  # noqa: E402
from loom.streaming.compiler import CompiledStorageSink  # noqa: E402
from loom.streaming.nodes._table import Backend, IntoTable  # noqa: E402
from loom.streaming.nodes._table.common import SqlAlchemySinkConfig  # noqa: E402
from tests.unit.streaming.compiler.cases import Result  # noqa: E402

pytestmark = pytest.mark.bytewax


class _Partition:
    def write_batch(self, items: Sequence[Any]) -> int:
        return len(items)

    def close(self) -> int:
        return 0


class _FakeCtx:
    def __init__(self, *, bridge: AsyncBridge | None) -> None:
        self.plan = type("_Plan", (), {"name": "test_flow"})()
        self.bridge = bridge
        self.flow_runtime = ObservabilityRuntime.noop()
        self.flow_run_id = "run-1"
        self.commit_tracker = None
        self._session_manager = object()
        self.session_manager_calls: list[DatabaseConfig] = []

    def session_manager_for(self, config: DatabaseConfig) -> object:
        self.session_manager_calls.append(config)
        return self._session_manager


class TestStorageDynamicSink:
    def test_sqlalchemy_table_partition_receives_async_bridge(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured: dict[str, object] = {}

        def _build_partition(
            self: IntoTable[Any],
            config: Any,
            worker_index: int,
            worker_count: int,
            bridge: AsyncBridge | None = None,
            session_manager: object | None = None,
            logger: object | None = None,
        ) -> _Partition:
            captured["config"] = config
            captured["worker_index"] = worker_index
            captured["worker_count"] = worker_count
            captured["bridge"] = bridge
            captured["session_manager"] = session_manager
            return _Partition()

        monkeypatch.setattr(IntoTable, "build_partition", _build_partition)

        node = IntoTable(
            payload=Result,
            table="results",
            backend=Backend.SQLALCHEMY,
            name="results_sink",
        )
        compiled = CompiledStorageSink(
            node=cast(Any, node),
            config=SqlAlchemySinkConfig.from_config(
                {"url": "sqlite:///:memory:", "table": "results", "chunk_size": 500},
                default_table="results",
            ),
            database_config=DatabaseConfig(
                url=Dsn("sqlite:///:memory:"), pool=PoolConfig(pre_ping=True)
            ),
        )
        bridge = cast(AsyncBridge, object())
        fake_ctx = cast(Any, _FakeCtx(bridge=bridge))
        dynamic_sink = cast(Any, _storage._StorageDynamicSink(compiled, fake_ctx))

        partition = dynamic_sink.build("step", 2, 3)

        assert isinstance(partition, _storage._StorageSinkPartition)
        assert isinstance(captured["config"], SqlAlchemySinkConfig)
        assert captured["config"].table == "results"
        assert captured["worker_index"] == 2
        assert captured["worker_count"] == 3
        assert captured["bridge"] is bridge
        fake_ctx = cast(Any, dynamic_sink._ctx)
        assert captured["session_manager"] is fake_ctx._session_manager
        assert fake_ctx.session_manager_calls == [
            DatabaseConfig(url=Dsn("sqlite:///:memory:"), pool=PoolConfig(pre_ping=True))
        ]

    def test_sqlalchemy_table_partition_requires_async_bridge(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _build_partition(
            self: IntoTable[Any],
            config: Any,
            worker_index: int,
            worker_count: int,
            bridge: AsyncBridge | None = None,
            session_manager: object | None = None,
            logger: object | None = None,
        ) -> _Partition:
            del self, config, worker_index, worker_count, bridge, session_manager, logger
            return _Partition()

        monkeypatch.setattr(IntoTable, "build_partition", _build_partition)

        node = IntoTable(
            payload=Result,
            table="results",
            backend=Backend.SQLALCHEMY,
            name="results_sink",
        )
        compiled = CompiledStorageSink(
            node=cast(Any, node),
            config=SqlAlchemySinkConfig.from_config(
                {"url": "sqlite:///:memory:", "table": "results", "chunk_size": 500},
                default_table="results",
            ),
            database_config=DatabaseConfig(url=Dsn("sqlite:///:memory:")),
        )
        dynamic_sink = _storage._StorageDynamicSink(compiled, cast(Any, _FakeCtx(bridge=None)))

        with pytest.raises(RuntimeError, match="requires an AsyncBridge"):
            dynamic_sink.build("step", 0, 1)
