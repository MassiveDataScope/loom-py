"""Unit tests for the ExpandRoutes fan-out node."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, ClassVar

import pytest

from loom.core.model import LoomStruct
from loom.streaming import Process
from loom.streaming.nodes._decompose import PayloadExpander
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._shape import Drain


class _StoreEvent(LoomStruct):
    store_id: str
    name: str


class AcmeExpandStoreRow(LoomStruct):
    store_id: str


class AcmeExpandAuditRow(LoomStruct):
    name: str


class _StoreExpander(PayloadExpander[_StoreEvent]):
    outputs: ClassVar[tuple[type, ...]] = (AcmeExpandStoreRow, AcmeExpandAuditRow)

    @classmethod
    def expand(cls, event: _StoreEvent) -> dict[type, list[Any]]:
        return {
            AcmeExpandStoreRow: [AcmeExpandStoreRow(store_id=event.store_id)],
            AcmeExpandAuditRow: [AcmeExpandAuditRow(name=event.name)],
        }


def _drain_process() -> Process[Any, Any]:
    return Process(Drain())


class TestExpandRoutes:
    def test_expand_routes_requires_routes_or_default(self) -> None:
        with pytest.raises(ValueError, match="at least one route"):
            ExpandRoutes(expander=_StoreExpander, routes={}, default=None)

    def test_expand_routes_with_default_only(self) -> None:
        default = _drain_process()
        node: ExpandRoutes[_StoreEvent] = ExpandRoutes(
            expander=_StoreExpander,
            routes={},
            default=default,
        )
        assert node.default is default
        assert len(node.routes) == 0

    def test_expand_routes_routes_are_frozen(self) -> None:
        node: ExpandRoutes[_StoreEvent] = ExpandRoutes(
            expander=_StoreExpander,
            routes={AcmeExpandStoreRow: _drain_process()},
        )
        assert isinstance(node.routes, MappingProxyType)
        with pytest.raises((TypeError, AttributeError)):
            node.routes[AcmeExpandAuditRow] = _drain_process()  # type: ignore[index]

    def test_expand_routes_stores_expander_and_routes(self) -> None:
        store_process = _drain_process()
        audit_process = _drain_process()
        node: ExpandRoutes[_StoreEvent] = ExpandRoutes(
            expander=_StoreExpander,
            routes={AcmeExpandStoreRow: store_process, AcmeExpandAuditRow: audit_process},
        )
        assert node.expander is _StoreExpander
        assert node.routes[AcmeExpandStoreRow] is store_process
        assert node.routes[AcmeExpandAuditRow] is audit_process
        assert node.default is None

    def test_expand_routes_expander_outputs_accessible_via_node(self) -> None:
        node: ExpandRoutes[_StoreEvent] = ExpandRoutes(
            expander=_StoreExpander,
            routes={AcmeExpandStoreRow: _drain_process()},
            default=_drain_process(),
        )
        outputs = node.expander.outputs
        assert AcmeExpandStoreRow in outputs
        assert AcmeExpandAuditRow in outputs
