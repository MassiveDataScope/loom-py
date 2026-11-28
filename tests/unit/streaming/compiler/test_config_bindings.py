"""Tests for resolving declarative config bindings in streaming flows."""

from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.core.model import LoomFrozenStruct
from loom.streaming import (
    ContextSupplier,
    Fork,
    FromTopic,
    IntoTopic,
    Message,
    Process,
    RecordStep,
    Route,
    Router,
    StreamFlow,
    With,
    msg,
)
from loom.streaming.compiler import CompilationError, compile_flow
from tests.unit.streaming.compiler.cases import Order, Result


class _ConfiguredStep(RecordStep[Order, Result]):
    """Step whose constructor is resolved from YAML bindings."""

    def __init__(self, *, prefix: str, suffix: str = "") -> None:
        self.prefix = prefix
        self.suffix = suffix

    def execute(self, message: Message[Order], **kwargs: object) -> Result:
        del kwargs
        return Result(value=f"{self.prefix}:{message.payload.order_id}{self.suffix}")


class _DeclaredStep(RecordStep[Order, Result]):
    """Step declared as a class in the flow and instantiated by the compiler."""

    def execute(self, message: Message[Order], **kwargs: object) -> Result:
        del kwargs
        return Result(value=message.payload.order_id)


class _TokenFactory(ContextSupplier):
    """Configured context factory resolved from YAML bindings."""

    __slots__ = ("token",)

    def __init__(self, *, token: str) -> None:
        self.token = token
        super().__init__(self._create)

    def _create(self) -> AbstractContextManager[str]:
        return nullcontext(self.token)


class _HttpxLimitsConfig(LoomFrozenStruct, frozen=True):
    """Nested limits schema resolved from the YAML section."""

    max_connections: int
    max_keepalive_connections: int


class _HttpxConfig(LoomFrozenStruct, frozen=True):
    """Structured config object resolved from a nested YAML section."""

    timeout_s: float
    limits: _HttpxLimitsConfig


class _StructuredFactory(ContextSupplier):
    """Factory resolved from a structured config plus a plain kwarg."""

    __slots__ = ("config", "label")

    def __init__(self, config: _HttpxConfig, *, label: str) -> None:
        self.config = config
        self.label = label
        super().__init__(self._create)

    def _create(self) -> AbstractContextManager[str]:
        return nullcontext(self.label)


class TestConfigBindings:
    """Binding resolution for declarative streaming flows."""

    def test_compile_flow_instantiates_declared_step_classes(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """Bare step classes should be materialized by the compiler."""
        runtime_config = streaming_kafka_config

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="declared_step_flow",
            source=FromTopic("orders.raw", payload=Order),
            process=Process(
                _DeclaredStep,
                IntoTopic("orders.validated", payload=Result),
            ),
        )

        plan = compile_flow(flow, config=runtime_config)

        node = plan.nodes[0].node
        assert isinstance(node, _DeclaredStep)

    def test_compile_flow_resolves_step_and_context_supplier_bindings(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """Bindings should be materialized before the plan is compiled."""
        merged_config = OmegaConf.merge(
            streaming_kafka_config,
            OmegaConf.create(
                {
                    "streaming": {
                        "bindings": {
                            "step": {
                                "prefix": "cfg-prefix",
                                "suffix": "cfg-suffix",
                            },
                            "factory": {
                                "token": "cfg-token",
                            },
                        }
                    }
                }
            ),
        )
        runtime_config = OmegaConf.create(OmegaConf.to_container(merged_config, resolve=False))
        assert isinstance(runtime_config, DictConfig)

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="binding_flow",
            source=FromTopic("orders.raw", payload=Order),
            process=Process(
                With(
                    process=Process(
                        _ConfiguredStep.from_config(
                            "streaming.bindings.step",
                            prefix="override-prefix",
                        ),
                        IntoTopic("orders.validated", payload=Result),
                    ),
                    token_factory=_TokenFactory.from_config("streaming.bindings.factory"),
                ),
            ),
        )

        plan = compile_flow(flow, config=runtime_config)

        scoped = plan.nodes[0].node
        assert isinstance(scoped, With)

        step = scoped.process.nodes[0]
        assert isinstance(step, _ConfiguredStep)
        assert step.prefix == "override-prefix"
        assert step.suffix == "cfg-suffix"

        factory = scoped.context_suppliers["token_factory"]
        assert isinstance(factory, _TokenFactory)
        assert factory.token == "cfg-token"

    def test_compile_flow_rejects_missing_required_binding_field(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """Missing required keys should fail binding resolution."""
        runtime_config = streaming_kafka_config.copy()
        OmegaConf.update(
            runtime_config,
            "streaming.bindings.bad_step",
            {"suffix": "cfg-suffix"},
            merge=False,
        )

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="missing_binding_flow",
            source=FromTopic("orders.raw", payload=Order),
            process=Process(
                _ConfiguredStep.from_config("streaming.bindings.bad_step"),
                IntoTopic("orders.validated", payload=Result),
            ),
        )

        with pytest.raises(
            CompilationError,
            match="requires config field 'prefix'",
        ):
            compile_flow(flow, config=runtime_config)

    def test_compile_flow_rejects_invalid_structured_binding_type(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """Invalid structured values should fail validation."""
        runtime_config = streaming_kafka_config.copy()
        OmegaConf.update(
            runtime_config,
            "streaming.bindings.bad_structured_factory",
            {
                "config": {
                    "timeout_s": "wrong",
                    "limits": {
                        "max_connections": 50,
                        "max_keepalive_connections": 20,
                    },
                },
                "label": "cfg-label",
            },
            merge=False,
        )

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="invalid_structured_binding_flow",
            source=FromTopic("orders.raw", payload=Order),
            process=Process(
                With(
                    process=Process(
                        _DeclaredStep,
                        IntoTopic("orders.validated", payload=Result),
                    ),
                    structured_factory=_StructuredFactory.from_config(
                        "streaming.bindings.bad_structured_factory",
                        label="override-label",
                    ),
                ),
            ),
        )

        with pytest.raises(CompilationError, match="Expected `float`, got `str`"):
            compile_flow(flow, config=runtime_config)

    def test_compile_flow_resolves_structured_constructor_bindings(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """Nested YAML keys should map to constructor field names."""
        runtime_config = streaming_kafka_config.copy()
        OmegaConf.update(
            runtime_config,
            "streaming.bindings.structured_factory",
            {
                "config": {
                    "timeout_s": 5.0,
                    "limits": {
                        "max_connections": 50,
                        "max_keepalive_connections": 20,
                    },
                },
                "label": "cfg-label",
            },
            merge=False,
        )
        assert isinstance(runtime_config, DictConfig)

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="structured_binding_flow",
            source=FromTopic("orders.raw", payload=Order),
            process=Process(
                With(
                    process=Process(
                        _DeclaredStep,
                        IntoTopic("orders.validated", payload=Result),
                    ),
                    structured_factory=_StructuredFactory.from_config(
                        "streaming.bindings.structured_factory",
                        label="override-label",
                    ),
                ),
            ),
        )

        plan = compile_flow(flow, config=runtime_config)

        scoped = plan.nodes[0].node
        assert isinstance(scoped, With)

        factory = scoped.context_suppliers["structured_factory"]
        assert isinstance(factory, _StructuredFactory)
        assert factory.label == "override-label"
        assert factory.config.timeout_s == pytest.approx(5.0)
        assert factory.config.limits.max_connections == 50
        assert factory.config.limits.max_keepalive_connections == 20

    def test_compile_flow_resolves_router_branch_bindings(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """Router branches should resolve bindings without dropping a family."""
        runtime_config = streaming_kafka_config.copy()
        OmegaConf.update(
            runtime_config,
            "streaming.bindings.step",
            {"prefix": "cfg-prefix", "suffix": "cfg-suffix"},
            merge=False,
        )

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="router_binding_flow",
            source=FromTopic("orders.raw", payload=Order),
            process=Process(
                Router(
                    selector=msg.payload.order_id,
                    routes={
                        "vip": Process(
                            _ConfiguredStep.from_config("streaming.bindings.step"),
                            IntoTopic("orders.a", payload=Result),
                        )
                    },
                    predicate_routes=(
                        Route(
                            when=msg.payload.order_id != "",
                            process=Process(
                                _ConfiguredStep.from_config("streaming.bindings.step"),
                                IntoTopic("orders.b", payload=Result),
                            ),
                        ),
                    ),
                ),
            ),
        )

        plan = compile_flow(flow, config=runtime_config)
        router = plan.nodes[0].node
        assert isinstance(router, Router)
        assert isinstance(router.routes["vip"].nodes[0], _ConfiguredStep)
        assert isinstance(router.predicate_routes[0].process.nodes[0], _ConfiguredStep)

    def test_compile_flow_resolves_fork_branch_bindings(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """Fork branches should resolve bindings without dropping a family."""
        runtime_config = streaming_kafka_config.copy()
        OmegaConf.update(
            runtime_config,
            "streaming.bindings.step",
            {"prefix": "cfg-prefix", "suffix": "cfg-suffix"},
            merge=False,
        )

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="fork_binding_flow",
            source=FromTopic("orders.raw", payload=Order),
            process=Process(
                Fork.by(
                    msg.payload.order_id,
                    branches={
                        "vip": Process(
                            _ConfiguredStep.from_config("streaming.bindings.step"),
                            IntoTopic("orders.a", payload=Result),
                        )
                    },
                ),
            ),
        )

        plan = compile_flow(flow, config=runtime_config)
        fork = plan.nodes[0].node
        assert isinstance(fork, Fork)
        assert isinstance(fork.routes["vip"].nodes[0], _ConfiguredStep)
