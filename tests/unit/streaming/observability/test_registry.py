"""Adapter node registry tests for streaming observability."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.streaming.bytewax._adapter import _NODE_HANDLERS  # noqa: E402
from loom.streaming.nodes._step import RecordStep  # noqa: E402


class TestNodeHandlerRegistry:
    def test_node_handlers_is_immutable(self) -> None:
        assert isinstance(_NODE_HANDLERS, MappingProxyType)

        with pytest.raises(TypeError):
            _NODE_HANDLERS[object] = lambda *a: None  # type: ignore[index]

    def test_node_handlers_cover_step_handlers(self) -> None:
        assert RecordStep in _NODE_HANDLERS
