"""The audit log's pinned shape and the revision an audited application needs (FR-018)."""

from __future__ import annotations

from loom.core.repository.sqlalchemy.rls.audit import (
    EXPIRED_AUDIT_PARTITIONS,
    OLDEST_EXPIRED_AUDIT_PARTITION,
)
from loom.core.repository.sqlalchemy.rls.guard_manifest import AUDIT_LOG_SHAPE
from loom.core.repository.sqlalchemy.rls.integrity import audit_shape_problems


def test_the_pinned_shape_has_no_problem() -> None:
    assert audit_shape_problems(AUDIT_LOG_SHAPE) == []


def test_a_missing_log_names_the_log() -> None:
    [problem] = audit_shape_problems(())

    assert (problem.check, problem.subject, problem.actual) == (
        "audit.shape",
        "audit_log",
        "missing",
    )


def test_a_changed_column_names_the_column() -> None:
    changed = tuple(
        "subject text" if line.startswith("subject ") else line for line in AUDIT_LOG_SHAPE
    )

    [problem] = audit_shape_problems(changed)

    assert problem.check == "audit.shape"
    assert "subject text not null" in problem.expected
    assert "subject text" in problem.actual


def test_an_added_column_is_a_problem() -> None:
    assert audit_shape_problems((*AUDIT_LOG_SHAPE, "note text")) != []


def test_a_dropped_index_names_the_index() -> None:
    dropped = tuple(line for line in AUDIT_LOG_SHAPE if "audit_log_trace_idx" not in line)

    [problem] = audit_shape_problems(dropped)

    assert "audit_log_trace_idx" in problem.expected
    assert problem.actual == "none"


def test_a_changed_primary_key_is_a_problem() -> None:
    changed = tuple(
        "constraint PRIMARY KEY (id)" if line == "constraint PRIMARY KEY (at, id)" else line
        for line in AUDIT_LOG_SHAPE
    )

    [problem] = audit_shape_problems(changed)

    assert problem.expected == "constraint PRIMARY KEY (at, id)"
    assert problem.actual == "constraint PRIMARY KEY (id)"


def test_drop_expired_takes_the_oldest_of_the_partitions_verify_lists() -> None:
    assert OLDEST_EXPIRED_AUDIT_PARTITION == EXPIRED_AUDIT_PARTITIONS + " LIMIT 1"
