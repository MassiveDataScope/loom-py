from __future__ import annotations

import pytest

from loom.core.routing import DefaultingRouteResolver, LogicalRef, as_logical_ref


class TestRouting:
    def test_logical_ref_normalizes_strings_and_rejects_empty_values(self) -> None:
        ref = as_logical_ref("orders-input")

        assert ref == LogicalRef("orders-input")
        assert ref.ref == "orders-input"
        assert str(ref) == "orders-input"

        with pytest.raises(ValueError, match="non-empty"):
            LogicalRef("")

    def test_defaulting_route_resolver_prefers_override_then_default(self) -> None:
        resolver = DefaultingRouteResolver(
            default="default-target",
            overrides={"orders": "orders-target"},
            kind="test target",
        )

        assert resolver.resolve("orders") == "orders-target"
        assert resolver.resolve(LogicalRef("missing")) == "default-target"

    def test_defaulting_route_resolver_raises_when_no_target_exists(self) -> None:
        resolver: DefaultingRouteResolver[str] = DefaultingRouteResolver(
            default=None,
            overrides={},
            kind="test target",
        )

        with pytest.raises(KeyError, match="orders"):
            resolver.resolve("orders")
