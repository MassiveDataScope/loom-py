"""Contracts for audit models and the reader port."""

from datetime import UTC, datetime
from typing import Any, get_type_hints

import msgspec
import pytest

from loom.core.audit import AuditEntry, AuditLogReader
from loom.core.audit.entry import audit_entry_for
from loom.core.config import ConfigError
from loom.core.model import BaseModel, NotMaterialized, get_column_fields
from loom.core.model.introspection import scalar_column_annotations
from loom.core.repository.abc import CursorResult, QuerySpec
from loom.core.repository.abc.cursor import cursor_carries


def test_audit_entry_columns_and_cursor_key() -> None:
    columns = get_column_fields(AuditEntry)
    assert tuple(columns) == (
        "at",
        "id",
        "op",
        "schema_name",
        "table",
        "row_key",
        "changed",
        "subject",
        "mechanism",
        "on_behalf_of",
        "trace_id",
        "elevated",
        "origin",
    )
    assert tuple(name for name, info in columns.items() if info.field.primary_key) == ("at", "id")
    hints = get_type_hints(AuditEntry)
    assert hints["at"] is datetime
    assert hints["id"] is int
    assert hints["changed"] == dict[str, Any]
    assert hints["elevated"] == tuple[str, ...]
    for name in (
        "op",
        "schema_name",
        "table",
        "row_key",
        "subject",
        "mechanism",
        "on_behalf_of",
        "trace_id",
        "origin",
    ):
        assert hints[name] is str
    for name in ("txid", "scopes", "db_user", "app_name", "client_addr", "backend_pid"):
        assert name not in columns
    scalars = scalar_column_annotations(AuditEntry)
    assert {"subject", "table", "row_key", "trace_id", "op", "at"} <= scalars.keys()
    assert "changed" not in scalars and "elevated" not in scalars
    assert cursor_carries(scalars["at"])
    assert cursor_carries(scalars["id"])


def test_audit_entry_response_uses_camel_case() -> None:
    entry = AuditEntry(
        at=datetime(2026, 1, 1, tzinfo=UTC),
        id=1,
        op="I",
        schema_name="public",
        table="ledger",
        row_key='{"id": 1}',
        changed={"amount": 2},
        subject="alice",
        mechanism="direct",
        on_behalf_of="bob",
        trace_id="t1",
        elevated=("admin",),
        origin="api",
    )
    response = msgspec.to_builtins(entry)
    assert response["schemaName"] == "public"
    assert response["rowKey"] == '{"id": 1}'
    assert response["onBehalfOf"] == "bob"
    assert response["traceId"] == "t1"


def test_audit_entry_for_caches_distinct_store_models() -> None:
    ledger = audit_entry_for("ledger")
    assert ledger is audit_entry_for("ledger")
    assert ledger is not audit_entry_for("archive")
    assert ledger.__name__ == "AuditEntry_ledger"
    assert ledger.__store__ == "ledger"
    assert issubclass(ledger, AuditEntry)
    assert issubclass(ledger, BaseModel)
    assert issubclass(ledger, NotMaterialized)


def test_audit_entry_for_rejects_invalid_store_key() -> None:
    with pytest.raises(ConfigError):
        audit_entry_for("bad-key")


def test_audit_log_reader_is_runtime_checkable_with_one_read_method() -> None:
    class Reader:
        async def read(self, query: QuerySpec, actors: object) -> CursorResult[AuditEntry]:
            return CursorResult(items=(), next_cursor=None, has_next=False)

    assert isinstance(Reader(), AuditLogReader)
    assert {name for name in AuditLogReader.__dict__ if not name.startswith("_")} == {"read"}
