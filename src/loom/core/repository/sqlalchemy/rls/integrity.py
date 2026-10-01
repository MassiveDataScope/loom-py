"""Checks that the guard in the database is the one this release of loom shipped.

The catalogue is readable by every role, so the application user can run them
at startup and ``verify`` can run them with any connection. They detect a
changed function, a changed configuration, owner or grant, an extra or missing
object, and a disabled or rebound event trigger.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.backend.scoped_ddl import MISSING_EVENT_TRIGGERS
from loom.core.repository.sqlalchemy.rls.guard_manifest import (
    GUARD_FUNCTIONS_SHA256,
    OWNER_FUNCTIONS,
    function_fingerprint,
)

GUARD_FUNCTIONS = (
    "SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS arguments, "
    "pg_get_function_result(p.oid) AS result, l.lanname, p.prosecdef, p.provolatile::text, "
    "p.proisstrict, p.proleakproof, p.proparallel::text, p.prosrc, "
    "coalesce(p.proconfig, ARRAY[]::text[]) AS proconfig, o.rolsuper AS owner_is_superuser, "
    "coalesce((SELECT array_agg(coalesce(g.rolname, 'PUBLIC') || ':' || a.privilege_type "
    "ORDER BY 1) FROM aclexplode(p.proacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee "
    "WHERE a.grantee <> p.proowner), ARRAY[]::text[]) AS grants "
    "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
    "JOIN pg_language l ON l.oid = p.prolang JOIN pg_roles o ON o.oid = p.proowner "
    "WHERE n.nspname = :guard"
)
_GUARD_FUNCTIONS = text(GUARD_FUNCTIONS)
_MISSING_EVENT_TRIGGERS = text(MISSING_EVENT_TRIGGERS)


@dataclass(frozen=True, slots=True)
class Problem:
    """One way the installed guard differs from the released one."""

    check: str
    subject: str
    expected: str
    actual: str


async def guard_problems(
    connection: AsyncConnection, guard: str, owner: str | None = None
) -> list[Problem]:
    """Return how the guard ``guard`` differs from the released one; empty when it matches.

    With ``owner`` the grants are compared exactly; without it only grants to
    ``PUBLIC`` are reported, which is what the application user can judge.
    """
    rows = list(await connection.execute(_GUARD_FUNCTIONS, {"guard": guard}))
    problems = function_problems(rows, guard, owner)
    triggers = await connection.execute(_MISSING_EVENT_TRIGGERS, {"guard": guard})
    problems += [
        Problem("guard.event_triggers", str(row[0]), "enabled always", "missing or changed")
        for row in triggers
    ]
    return problems


def function_problems(rows: Sequence[Any], guard: str, owner: str | None) -> list[Problem]:
    """Compare the guard's functions with the released fingerprint, configuration and grants."""
    if not rows:
        return [Problem("guard.functions", guard, "installed", "missing")]
    problems: list[Problem] = []
    actual = function_fingerprint(rows)
    if actual != GUARD_FUNCTIONS_SHA256:
        problems.append(Problem("guard.functions", guard, GUARD_FUNCTIONS_SHA256, actual))
    expected_config = [f"search_path={guard}, pg_catalog, pg_temp"]
    for row in rows:
        name = f"{row.proname}({row.arguments})"
        if list(row.proconfig) != expected_config:
            problems.append(
                Problem("guard.function_config", name, str(expected_config), str(row.proconfig))
            )
        if not row.owner_is_superuser:
            problems.append(Problem("guard.function_owner", name, "superuser", "other"))
        problems += _grant_problems(row, name, owner)
    return problems


def _grant_problems(row: Any, name: str, owner: str | None) -> list[Problem]:
    grants = set(row.grants)
    if owner is None:
        public = {grant for grant in grants if grant.startswith("PUBLIC:")}
        return [Problem("guard.function_grants", name, "[]", str(sorted(public)))] if public else []
    expected = {f"{owner}:EXECUTE"} if row.proname in OWNER_FUNCTIONS else set()
    if grants != expected:
        return [Problem("guard.function_grants", name, str(sorted(expected)), str(sorted(grants)))]
    return []
