"""The guard revisions this release of loom ships, each pinned by its SHA-256 and its catalogue.

A released revision is never edited; a change is a new revision. The file
digest is checked before a revision runs and against what the database
recorded; the catalogue digests identify which revision a database holds from
what any role can read, so startup accepts every revision at or above
:data:`MIN_COMPATIBLE_GUARD_REVISION`. Upgrade the application first, then
bootstrap.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from importlib.resources import files
from typing import Final

from loom.core.config import ConfigError

_GUARD: Final = files("loom.core.repository.sqlalchemy.rls") / "guard"
PREFLIGHT_FILE: Final = "preflight.sql"
PREFLIGHT_SHA256: Final = "32a96565d1717b06454384783ba33a54264997dfb2233cb6c7f18a9dbd16a162"


@dataclass(frozen=True, slots=True)
class GuardRevision:
    """One static guard revision: its number, its file, its digest and its catalogue digests."""

    number: int
    filename: str
    sha256: str
    catalog: Mapping[str, str] = field(hash=False)

    def sql(self) -> str:
        """Return the revision's text after checking it is the released text.

        Raises:
            ConfigError: When the packaged file differs from the released digest.
        """
        return _verified(self.filename, self.sha256)


def digest(text: str) -> str:
    """Return the hexadecimal SHA-256 of ``text`` encoded as UTF-8."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def catalog_digest(lines: Iterable[str]) -> str:
    """Digest catalogue lines independently of their order."""
    return digest("\x1e".join(sorted(lines)))


def preflight_sql() -> str:
    """Return the static statement that defines the guard-creating temporary function.

    Raises:
        ConfigError: When the packaged file differs from the released digest.
    """
    return _verified(PREFLIGHT_FILE, PREFLIGHT_SHA256)


def _verified(filename: str, sha256: str) -> str:
    text = (_GUARD / filename).read_text(encoding="utf-8")
    if digest(text) != sha256:
        raise ConfigError(f"guard file {filename} differs from the released one")
    return text


GUARD_REVISIONS: Final[tuple[GuardRevision, ...]] = (
    GuardRevision(
        1,
        "0001.sql",
        "1c411a519c0d9a5bd5ec2b04d83cc822673eadf28ec2d5f6b44255b228f1583a",
        {
            "functions": "207b276a1fb5cb6881da6e30a542c8e1f176228787e18dcdec8d74c353f2d711",
            "relations": "13ac502176825abf96ee3cef7fe718e50adbb1dedb80ad86790b554c572273fb",
            "columns": "17e334109a9b8d44e4dd02b96ecbb888bcb456da2f91e97c3d855f8ce1c642b1",
            "constraints": "dd6f74082263bed44a74c4afc2697726ed1336a61e020b05711033202dc7fe2d",
            "triggers": "133e23ee841b691af4b325b321af86e78fdb5f164183c913c43977149c5e3a68",
            "objects": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        },
    ),
)
MIN_COMPATIBLE_GUARD_REVISION: Final = 1
OWNER_FUNCTIONS: Final = frozenset(
    {
        "protect_scoped_table",
        "unprotect_scoped_table",
        "open_hatch",
        "assert_scoped_schema",
        "grant_table",
        "grant_privileges",
        "check_privileges",
        "prepare_version_tables",
        "limit_version_table",
        "registered_tables",
        "guard_schema",
        "app_schema",
        "owner_role",
        "group_role",
        "users_of",
        "tagged",
        "reject",
        "refuse",
        "is_member",
        "commands",
        "authorize",
    }
)
OWNER_TABLES: Final = frozenset({"config", "scoped_table", "scoped_policy"})
PINNED_SETTINGS: Final[Mapping[str, tuple[str, ...]]] = {
    "set_password_verifier": ("log_statement=none",)
}


def pending_revisions(applied: Mapping[int, str]) -> tuple[GuardRevision, ...]:
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
