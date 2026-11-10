"""``verify``'s checks of audit reading (spec 009, review M8, SC-003, marker sites 10 and 12)."""

from __future__ import annotations

import dataclasses
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from sqlalchemy import MetaData

from loom.core.audit.entry import audit_entry_for
from loom.core.locator import Application
from loom.core.model import BaseModel, ColumnField
from loom.core.model.privilege import READ_WRITE
from loom.core.model.redaction import Dsn
from loom.core.persistence.database import DatabaseConfig
from loom.core.repository.sqlalchemy.rls.audit_verify import reader_findings
from loom.core.repository.sqlalchemy.rls.config import (
    BootstrapConfig,
    DatabaseRoles,
    DatabaseUser,
)
from loom.core.repository.sqlalchemy.rls.findings import Acl, Finding
from loom.core.repository.sqlalchemy.rls.guard_manifest import READ_AUDIT_LOG_ARGUMENTS
from loom.core.repository.sqlalchemy.rls.verify import (
    _audit_reading_findings,
    _global_privileges,
)
from loom.core.schema_names import SchemaNames

SOURCE = "-- the released reader body"
RESULT = (
    "TABLE(id bigint, at timestamp with time zone, op text, schema_name text, table_name text, "
    "row_key text, changed jsonb, subject text, mechanism text, on_behalf_of text, "
    "trace_id text, elevated text[], origin text)"
)
TENANT = ("tenant", "character varying")


def _bootstrap(**overrides: Any) -> BootstrapConfig:
    config = BootstrapConfig(
        schema="s1",
        roles=DatabaseRoles(owner="s1_owner", migrator="s1_migrator"),
        database_users={"s1_rw": DatabaseUser(login=True, access="write")},
        names=SchemaNames.derived("s1"),
    )
    return dataclasses.replace(config, **overrides)


@dataclass(frozen=True, slots=True)
class _Config:
    """The reading columns of the guard's config row as ``GUARD_CONFIG`` reads them."""

    audit_reading: bool | None = True
    boundary_scope: str | None = TENANT[0]
    boundary_type: str | None = TENANT[1]


@dataclass(frozen=True, slots=True)
class _Reader:
    """One ``<schema>.read_audit_log`` as the catalogue holds it."""

    arguments: str = READ_AUDIT_LOG_ARGUMENTS
    owned: bool = True
    definer: bool = True
    config: tuple[str, ...] = ("search_path=pg_catalog, s1, pg_temp",)
    grants: tuple[str, ...] = ("s1_readers:EXECUTE",)
    executable: bool = False
    source: str = SOURCE
    search_path: str = "search_path=pg_catalog, s1, pg_temp"
    volatility: str = "s"
    language: str = "plpgsql"
    result: str = RESULT


def _checks(findings: list[Finding]) -> list[str]:
    return [finding.check for finding in findings]


READING = _bootstrap(audit_reading=True, audit_boundary=TENANT)
NOT_READING = _bootstrap()


def test_the_recorded_flag_and_boundary_match_the_declaration() -> None:
    assert _audit_reading_findings(_Config(), READING) == []


def test_no_config_row_reports_nothing_more() -> None:
    assert _audit_reading_findings(None, READING) == []


def test_a_recorded_flag_that_differs_is_drift() -> None:
    assert _checks(_audit_reading_findings(_Config(audit_reading=False), READING)) == [
        "guard.audit_reading"
    ]
    assert _checks(_audit_reading_findings(_Config(), NOT_READING)) == ["guard.audit_reading"]


def test_a_revision_three_guard_has_no_flag_to_record() -> None:
    older = _Config(audit_reading=None, boundary_scope=None, boundary_type=None)

    assert _audit_reading_findings(older, NOT_READING) == []
    assert set(_checks(_audit_reading_findings(older, READING))) == {"guard.audit_reading"}


def test_a_recorded_boundary_that_differs_while_reading_is_drift() -> None:
    other = _Config(boundary_scope="region", boundary_type="integer")
    missing = _Config(boundary_scope=None, boundary_type=None)

    assert _checks(_audit_reading_findings(other, READING)) == ["guard.audit_reading"]
    assert _checks(_audit_reading_findings(missing, READING)) == ["guard.audit_reading"]


def test_a_boundary_kept_while_reading_is_off_is_not_drift() -> None:
    assert _audit_reading_findings(_Config(audit_reading=False), NOT_READING) == []


def test_the_released_reader_granted_to_the_readers_has_no_finding() -> None:
    assert reader_findings([_Reader()], SOURCE, READING) == []


def test_the_reader_without_reading_is_granted_to_nobody() -> None:
    assert reader_findings([_Reader(grants=())], SOURCE, NOT_READING) == []
    assert _checks(reader_findings([_Reader()], SOURCE, NOT_READING)) == ["audit.reader_grants"]


def test_an_extra_grantee_is_drift() -> None:
    extra = _Reader(grants=("s1_readers:EXECUTE", "s1_ops:EXECUTE"))

    [finding] = reader_findings([extra], SOURCE, READING)

    assert finding.check == "audit.reader_grants"
    assert "s1_ops:EXECUTE" in finding.actual


def test_a_grant_with_grant_option_is_drift() -> None:
    grantable = _Reader(grants=("s1_readers:EXECUTE (grantable)",))

    assert _checks(reader_findings([grantable], SOURCE, READING)) == ["audit.reader_grants"]


def test_a_changed_body_is_drift() -> None:
    changed = _Reader(source=SOURCE + " -- and more")

    assert _checks(reader_findings([changed], SOURCE, READING)) == ["audit.reader_source"]


def test_a_reader_on_a_guard_without_the_released_source_is_drift() -> None:
    assert _checks(reader_findings([_Reader()], None, READING)) == ["audit.reader_source"]


def test_a_changed_shape_is_drift() -> None:
    assert _checks(reader_findings([_Reader(definer=False)], SOURCE, READING)) == [
        "audit.reader_shape"
    ]


def test_reading_without_a_reader_names_the_missing_function() -> None:
    [finding] = reader_findings([], SOURCE, READING)

    assert (finding.table, finding.check, finding.actual) == (
        "read_audit_log",
        "audit.reader",
        "missing",
    )
    assert reader_findings([], None, NOT_READING) == []


class _Ledger(BaseModel):
    __tablename__ = "ledgers"
    __privileges__ = {"writers": READ_WRITE}
    id: int = ColumnField(primary_key=True)


def test_global_privileges_skip_a_model_that_is_never_a_table() -> None:
    """Marker sites 10 and 12: the audit model reaches no table lookup and no privilege."""
    acl: Acl = defaultdict(lambda: defaultdict(set))
    acl["ledgers"]["s1_writers"] = {"INSERT", "UPDATE", "DELETE"}
    acl["ledgers"]["s1_readers"] = set()
    application = Application(
        models=(audit_entry_for("default"), _Ledger),
        metadata=MetaData(),
        database=DatabaseConfig(url=Dsn("postgresql+asyncpg://localhost/db")),
        bootstrap=NOT_READING,
        scoped={},
        scope_sources={},
    )

    findings = _global_privileges(acl, application, NOT_READING)

    assert {finding.table for finding in findings} <= {"ledgers"}
    assert "AuditEntry_default" not in str(findings)
