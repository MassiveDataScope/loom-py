"""Which audited tables record rows, and which guard revision a store needs."""

from __future__ import annotations

from loom.core.model.audited import AuditedTable
from loom.core.model.redaction import Dsn
from loom.core.stores.database import AuditSchemaConfig, DatabaseConfig, SchemaConfig
from loom.stores.sqlalchemy.alembic import StoreSchema
from loom.stores.sqlalchemy.rls.guard_manifest import (
    AUDIT_GUARD_REVISION,
    required_guard_revision,
)

_LEDGER = AuditedTable(schema=None, name="ledger", key_columns=("id",), excluded=frozenset())


def _application(*, enabled: bool) -> StoreSchema:
    from sqlalchemy import MetaData

    schema = SchemaConfig(audit=AuditSchemaConfig(enabled=enabled))
    return StoreSchema(
        models=(),
        metadata=MetaData(),
        database=DatabaseConfig(url=Dsn("postgresql+asyncpg://u:p@localhost/db"), schema=schema),
        bootstrap=None,
        scoped={},
        scope_sources={},
        audited={(None, "ledger"): _LEDGER},
    )


class TestActiveAudited:
    def test_marked_tables_record_while_the_switch_is_on(self) -> None:
        assert _application(enabled=True).recording_audit_tables == {(None, "ledger"): _LEDGER}

    def test_no_table_records_while_the_switch_is_off(self) -> None:
        application = _application(enabled=False)

        assert application.recording_audit_tables == {}
        assert application.audited  # the marks stay; only recording stops

    def test_an_auditing_store_needs_the_audit_revision(self) -> None:
        assert required_guard_revision(True) == AUDIT_GUARD_REVISION

    def test_a_store_without_audit_takes_any_revision(self) -> None:
        assert required_guard_revision(False) is None
