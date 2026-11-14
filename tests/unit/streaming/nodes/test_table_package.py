"""Tests for the public streaming table sink package facade."""

from __future__ import annotations

from loom.streaming.nodes._table import (
    Backend,
    DeltaSinkConfig,
    IntoTable,
    SqlAlchemySinkConfig,
)


class TestTablePackage:
    def test_table_package_exports_public_contract(self) -> None:
        assert IntoTable.__module__ == "loom.streaming.nodes._table.common"
        assert Backend.__module__ == "loom.streaming.nodes._table.common"
        assert SqlAlchemySinkConfig.__module__ == "loom.streaming.nodes._table.common"
        assert DeltaSinkConfig.__module__ == "loom.streaming.nodes._table.common"
