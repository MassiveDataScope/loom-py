"""``stores.<name>.schema.audit`` decodes with safe defaults and refuses what makes no sense."""

from __future__ import annotations

import msgspec
import pytest

from loom.core.persistence.database import AuditSchemaConfig, SchemaConfig


def test_auditing_is_on_by_default_with_no_retention_and_the_derived_reader() -> None:
    audit = msgspec.convert({}, SchemaConfig).audit

    assert audit == AuditSchemaConfig(enabled=True, retention_months=None, reader=None)


def test_the_audit_section_decodes_every_key() -> None:
    schema = msgspec.convert(
        {"audit": {"enabled": False, "retention_months": 24, "reader": "app_auditor"}}, SchemaConfig
    )

    assert schema.audit == AuditSchemaConfig(enabled=False, retention_months=24, reader="app_auditor")


@pytest.mark.parametrize("months", [0, -1])
def test_a_retention_below_one_month_is_refused(months: int) -> None:
    with pytest.raises(msgspec.ValidationError):
        msgspec.convert({"audit": {"retention_months": months}}, SchemaConfig)


def test_an_unknown_audit_key_is_refused() -> None:
    with pytest.raises(msgspec.ValidationError, match="retention"):
        msgspec.convert({"audit": {"retention": 3}}, SchemaConfig)
