"""The audit log's partition horizon: verify fails short of it, startup only warns."""

from __future__ import annotations

import logging

import pytest

from loom.core.stores.catalog import StoreKey
from loom.core.stores.event import StoresKey
from loom.stores.sqlalchemy.event import SqlalchemyEvent
from loom.stores.sqlalchemy.plugin import _warn_audit_horizon
from loom.stores.sqlalchemy.rls.audit_verify import AuditState, audit_findings


class TestAuditHorizon:
    @pytest.mark.parametrize(
        ("covered", "check", "expected"),
        [
            (0, "audit.partition", "a partition for this month"),
            (1, "audit.partition", "a partition for next month"),
            (2, "audit.horizon", "at least 3 months of partitions"),
        ],
    )
    def test_verify_reports_a_short_horizon(self, covered: int, check: str, expected: str) -> None:
        state = AuditState(
            supported=True, relations=frozenset({"audit_log"}), months_covered=covered
        )

        findings = audit_findings(state, {})

        assert [(f.check, f.expected) for f in findings] == [(check, expected)]

    def test_verify_accepts_three_months_or_more(self) -> None:
        state = AuditState(supported=True, relations=frozenset({"audit_log"}), months_covered=3)

        assert audit_findings(state, {}) == []

    @pytest.mark.parametrize(
        ("covered", "level", "event"),
        [
            (0, logging.ERROR, SqlalchemyEvent.AUDIT_PARTITION_MISSING),
            (2, logging.WARNING, SqlalchemyEvent.AUDIT_PARTITIONS_NEARLY_EXHAUSTED),
        ],
    )
    def test_startup_logs_a_short_horizon(
        self,
        caplog: pytest.LogCaptureFixture,
        covered: int,
        level: int,
        event: SqlalchemyEvent,
    ) -> None:
        with caplog.at_level(logging.WARNING):
            _warn_audit_horizon(StoreKey("default"), covered)

        assert [(record.levelno, record.msg) for record in caplog.records] == [(level, event)]
        assert caplog.records[0].__dict__[StoresKey.STORE] == "default"

    @pytest.mark.parametrize("covered", [None, 3, 13])
    def test_startup_is_silent_without_a_log_or_with_margin(
        self, caplog: pytest.LogCaptureFixture, covered: int | None
    ) -> None:
        with caplog.at_level(logging.WARNING):
            _warn_audit_horizon(StoreKey("default"), covered)

        assert caplog.records == []
