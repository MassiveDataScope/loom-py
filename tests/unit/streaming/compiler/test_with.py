"""Compiler validation for With / WithAsync resource scoping."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.streaming import (
    CollectBatch,
    ContextSupplier,
    FromTopic,
    IntoTopic,
    Process,
    ResourceScope,
    StreamFlow,
    With,
    WithAsync,
)
from loom.streaming.compiler import CompilationError, compile_flow
from tests.unit.streaming.compiler.cases import FakeStep, Order, Result


class TestWithCompiler:
    def test_compile_fails_on_batch_scope_with_direct_context_manager(
        self,
    ) -> None:
        """BATCH scope with a direct CM instance must be rejected at compile time."""

        class _FakeSyncCM:
            def __enter__(self) -> _FakeSyncCM:
                return self

            def __exit__(self, *args: object) -> None:
                return None

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                With(
                    process=Process(FakeStep(), IntoTopic("out", payload=Result)),
                    scope=ResourceScope.BATCH,
                    db=_FakeSyncCM(),
                )
            ),
        )

        config = OmegaConf.create({})
        with pytest.raises(CompilationError, match="ContextSupplier"):
            compile_flow(flow, config=config)

    def test_compile_succeeds_on_batch_scope_with_context_supplier(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """BATCH scope with a ContextSupplier is valid."""

        class _FakeSyncCM:
            def __enter__(self) -> _FakeSyncCM:
                return self

            def __exit__(self, *args: object) -> None:
                return None

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                With(
                    process=Process(FakeStep(), IntoTopic("out", payload=Result)),
                    scope=ResourceScope.BATCH,
                    db=ContextSupplier(lambda: _FakeSyncCM()),
                )
            ),
        )

        plan = compile_flow(flow, config=streaming_kafka_config)

        assert plan.name == "test"

    def test_compile_rejects_collect_batch_inside_with_async(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """WithAsync(process=...) only supports record steps and an optional terminal IntoTopic."""

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                WithAsync(
                    process=Process(
                        CollectBatch(max_records=2, timeout_ms=500),
                        IntoTopic("out", payload=Result),
                    )
                )
            ),
        )

        with pytest.raises(
            CompilationError, match="WithAsync\\(process=\\.\\.\\.\\) only supports"
        ):
            compile_flow(flow, config=streaming_kafka_config)
