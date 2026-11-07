"""The audit log's pinned shape and the revision an audited application needs (FR-018)."""

from __future__ import annotations

from loom.core.repository.sqlalchemy.rls.guard_manifest import AUDIT_LOG_COLUMNS
from loom.core.repository.sqlalchemy.rls.integrity import audit_shape_problems


def test_the_pinned_shape_has_no_problem() -> None:
    assert audit_shape_problems(AUDIT_LOG_COLUMNS) == []


def test_a_missing_log_names_the_log() -> None:
    [problem] = audit_shape_problems(())

    assert (problem.check, problem.subject, problem.actual) == (
        "audit.shape",
        "audit_log",
        "missing",
    )


def test_a_changed_column_names_the_column() -> None:
    changed = tuple(
        "subject text" if line.startswith("subject ") else line for line in AUDIT_LOG_COLUMNS
    )

    [problem] = audit_shape_problems(changed)

    assert problem.check == "audit.shape"
    assert "subject text not null" in problem.expected
    assert "subject text" in problem.actual


def test_an_added_column_is_a_problem() -> None:
    assert audit_shape_problems((*AUDIT_LOG_COLUMNS, "note text")) != []
