"""The audit log's partition horizon: verify fails short of it, startup only warns."""

from __future__ import annotations

import logging

import pytest

from loom.core.repository.sqlalchemy.backend import _warn_audit_horizon
from loom.core.repository.sqlalchemy.rls.audit_verify import AuditState, audit_findings


@pytest.mark.parametrize(
    ("covered", "check", "expected"),
    [
        (0, "audit.partition", "a partition for this month"),
        (1, "audit.partition", "a partition for next month"),
        (2, "audit.horizon", "at least 3 months of partitions"),
    ],
)
def test_verify_reports_a_short_horizon(covered: int, check: str, expected: str) -> None:
    state = AuditState(supported=True, relations=frozenset({"audit_log"}), months_covered=covered)

    findings = audit_findings(state, {})

    assert [(f.check, f.expected) for f in findings] == [(check, expected)]


def test_verify_accepts_three_months_or_more() -> None:
    state = AuditState(supported=True, relations=frozenset({"audit_log"}), months_covered=3)

    assert audit_findings(state, {}) == []


@pytest.mark.parametrize(
    ("covered", "level"),
    [(0, logging.ERROR), (2, logging.WARNING)],
)
def test_startup_logs_a_short_horizon(
    caplog: pytest.LogCaptureFixture, covered: int, level: int
) -> None:
    with caplog.at_level(logging.WARNING):
        _warn_audit_horizon("stores.default", covered)

    assert [record.levelno for record in caplog.records] == [level]
    assert "ensure-partitions" in caplog.records[0].getMessage()


@pytest.mark.parametrize("covered", [None, 3, 13])
def test_startup_is_silent_without_a_log_or_with_margin(
    caplog: pytest.LogCaptureFixture, covered: int | None
) -> None:
    with caplog.at_level(logging.WARNING):
        _warn_audit_horizon("stores.default", covered)

    assert caplog.records == []
