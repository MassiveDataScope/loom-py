"""The guard revisions this release of loom ships, each pinned by its SHA-256.

A released revision is never edited; a change is a new revision. The digest is
checked before a revision runs and against what the database recorded.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from importlib.resources import files
from typing import Any

from loom.core.config import ConfigError

_GUARD = files("loom.core.repository.sqlalchemy.rls") / "guard"
PREFLIGHT_FILE = "preflight.sql"
PREFLIGHT_SHA256 = "e409f2c9cbefc6e5e5a8929d42a8f31bffb0c9690c0280e3f391710b6cc5de7f"


@dataclass(frozen=True, slots=True)
class GuardRevision:
    """One static guard revision: its number, its file and the digest it was released with."""

    number: int
    filename: str
    sha256: str

    def sql(self) -> str:
        """Return the revision's text after checking it is the released text.

        Raises:
            ConfigError: When the packaged file differs from the released digest.
        """
        text = (_GUARD / self.filename).read_text(encoding="utf-8")
        if digest(text) != self.sha256:
            raise ConfigError(
                f"guard revision {self.number} ({self.filename}) differs from the released one"
            )
        return text


def digest(text: str) -> str:
    """Return the hexadecimal SHA-256 of ``text`` encoded as UTF-8."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def preflight_sql() -> str:
    """Return the static statement that defines the guard-creating temporary function.

    Raises:
        ConfigError: When the packaged file differs from the released digest.
    """
    text = (_GUARD / PREFLIGHT_FILE).read_text(encoding="utf-8")
    if digest(text) != PREFLIGHT_SHA256:
        raise ConfigError(f"{PREFLIGHT_FILE} differs from the released one")
    return text


GUARD_REVISIONS: tuple[GuardRevision, ...] = (
    GuardRevision(
        1, "0001.sql", "3a36281c4d30134013741ebfd6674e475e6372e5ee4b1c0112361194c63deb20"
    ),
)
REQUIRED_GUARD_REVISION = GUARD_REVISIONS[-1].number
GUARD_FUNCTIONS_SHA256 = "e18bc5f17d523dbe3d002d5cce20e1c1233dda06fff6960e2ab8ca04487bf765"
OWNER_FUNCTIONS = frozenset(
    {
        "protect_scoped_table",
        "unprotect_scoped_table",
        "open_hatch",
        "assert_scoped_schema",
        "grant_table",
        "prepare_version_tables",
        "registered_tables",
        "owner_functions",
        "app_schema",
        "owner_role",
        "group_role",
        "fail",
        "reject",
        "refuse",
        "violation",
        "is_member",
        "commands",
        "authorize",
        "set_hatch",
    }
)
GUARD_RELATIONS = frozenset(
    {
        "config",
        "config_pkey",
        "revision",
        "revision_pkey",
        "scoped_policy",
        "scoped_policy_pkey",
        "scoped_table",
        "scoped_table_pkey",
    }
)
_FINGERPRINT_COLUMNS = (
    "proname",
    "arguments",
    "result",
    "lanname",
    "prosecdef",
    "provolatile",
    "proisstrict",
    "proleakproof",
    "proparallel",
    "prosrc",
)


def function_fingerprint(rows: Iterable[Any]) -> str:
    """Digest the guard's functions as the catalogue stores them, independent of the schema name.

    Covers name, arguments, result, language, security, volatility, strictness,
    leakproofness, parallel safety and source; configuration, owner and grants
    are checked separately because they name the schema and its roles.
    """
    canonical = sorted(
        "\x1f".join(str(getattr(row, column)) for column in _FINGERPRINT_COLUMNS) for row in rows
    )
    return digest("\x1e".join(canonical))


def pending_revisions(applied: dict[int, str]) -> tuple[GuardRevision, ...]:
    """Return the revisions still to apply after checking the ones already applied.

    Raises:
        ConfigError: When the database holds a revision this release does not
            know, or one whose recorded digest differs from the released one.
    """
    known = {revision.number: revision for revision in GUARD_REVISIONS}
    unknown = sorted(set(applied) - set(known))
    if unknown:
        raise ConfigError(
            f"the guard holds revisions {unknown} this release of loom does not know; "
            "upgrade loom before bootstrapping this schema"
        )
    for number, recorded in applied.items():
        if recorded != known[number].sha256:
            raise ConfigError(f"guard revision {number} was recorded with a different digest")
    return tuple(revision for revision in GUARD_REVISIONS if revision.number not in applied)
