"""What ``verify`` reports: one difference between the declaration and the database."""

from __future__ import annotations

from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from loom.core.repository.sqlalchemy.rls.integrity import Problem

# A relation's grants as the catalogue holds them: relation, grantee, privileges.
Acl = dict[str, dict[str, set[str]]]


@dataclass(frozen=True, slots=True)
class Finding:
    """One difference between the declaration and the database."""

    table: str | None
    check: str
    expected: str
    actual: str


def diff(
    table: str | None, check: str, subject: str, wanted: AbstractSet[str], actual: AbstractSet[str]
) -> Finding:
    """A finding that shows both sets sorted, so reports compare across runs."""
    return Finding(table, check, f"{subject}: {sorted(wanted)}", f"{subject}: {sorted(actual)}")


def problem_finding(problem: Problem, table: str | None = None) -> Finding:
    """The finding ``verify`` reports for a problem startup would report, its subject kept."""
    return Finding(
        table,
        problem.check,
        f"{problem.subject}: {problem.expected}",
        f"{problem.subject}: {problem.actual}",
    )
