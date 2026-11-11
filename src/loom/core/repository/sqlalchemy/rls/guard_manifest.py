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
PREFLIGHT_SHA256: Final = "46b1fd415bdc8d9c359d9dc73962eef0f5bbe8bc3f6f1fe77669d101aeb875ba"


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


_GUARD_TRIGGERS: Final = "133e23ee841b691af4b325b321af86e78fdb5f164183c913c43977149c5e3a68"
"""No revision changes the guard's own triggers."""
_NO_OBJECTS: Final = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
"""The digest of no catalogue lines: the guard owns no other kind of object."""


def _catalog(*, functions: str, relations: str, columns: str, constraints: str) -> dict[str, str]:
    """A revision's catalogue digests, by category."""
    return {
        "functions": functions,
        "relations": relations,
        "columns": columns,
        "constraints": constraints,
        "triggers": _GUARD_TRIGGERS,
        "objects": _NO_OBJECTS,
    }


GUARD_REVISIONS: Final[tuple[GuardRevision, ...]] = (
    GuardRevision(
        1,
        "0001.sql",
        "1c411a519c0d9a5bd5ec2b04d83cc822673eadf28ec2d5f6b44255b228f1583a",
        _catalog(
            functions="207b276a1fb5cb6881da6e30a542c8e1f176228787e18dcdec8d74c353f2d711",
            relations="13ac502176825abf96ee3cef7fe718e50adbb1dedb80ad86790b554c572273fb",
            columns="17e334109a9b8d44e4dd02b96ecbb888bcb456da2f91e97c3d855f8ce1c642b1",
            constraints="dd6f74082263bed44a74c4afc2697726ed1336a61e020b05711033202dc7fe2d",
        ),
    ),
    GuardRevision(
        2,
        "0002.sql",
        "a1bc218a4b30bfec7119b1b9560afe04f53e132d303709de68384b710d7196a9",
        _catalog(
            functions="137f1619c20a465add893360e2102e2b7012bcadd5e4eb1e53343b7081f2dbb2",
            relations="13ac502176825abf96ee3cef7fe718e50adbb1dedb80ad86790b554c572273fb",
            columns="17e334109a9b8d44e4dd02b96ecbb888bcb456da2f91e97c3d855f8ce1c642b1",
            constraints="dd6f74082263bed44a74c4afc2697726ed1336a61e020b05711033202dc7fe2d",
        ),
    ),
    GuardRevision(
        3,
        "0003.sql",
        "1dfbf550952405b2c224dcc7a78d8d9ec81c545c9d1335ecbadf87fc8182b9ed",
        _catalog(
            functions="1065dd8a793faa698d42f38a81e6d90c248e86cd7698d9ff433c693e30072118",
            relations="d89b2c29543840c6d4c8383026b6d2e7000dff1535cdf0ce83db0d5943658dce",
            columns="7244d4f6ba3cd372296a3d963125966f5bb7ce3880aacd3a3419f3d398b138ab",
            constraints="7aac8c7b89099f3b5e15d23acfb33f1b2cedf4312a1c2fc919acc130b9ce29d7",
        ),
    ),
    GuardRevision(
        4,
        "0004.sql",
        "fd001c6245dfd0288392c62c3358f3bd57b6f31da949074fef44aa578d11ff56",
        _catalog(
            functions="1a4f971f5063c94ba854b9de5c9f23d8b77375fa493a9373517630af76844b3f",
            relations="d89b2c29543840c6d4c8383026b6d2e7000dff1535cdf0ce83db0d5943658dce",
            columns="87dbc2dd4ee8e78188c5c07a5b74b16a94ee899d7e727df42d1c582719d47389",
            constraints="422e6d0676b54eb6bb74ff9c0d8c797b8710befe461474aba98b18c75069f5e2",
        ),
    ),
)
MIN_COMPATIBLE_GUARD_REVISION: Final = 1
PARTITION_GUARD_REVISION: Final = 2
AUDIT_GUARD_REVISION: Final = 3
READING_GUARD_REVISION: Final = 4


def required_guard_revision(audited: bool, reading: bool = False) -> int | None:
    """The lowest guard revision a store needs.

    The audit reader's when it reads the audit log, the audit log's when it
    audits, else any.
    """
    if reading:
        return READING_GUARD_REVISION
    return AUDIT_GUARD_REVISION if audited else None


# The identity arguments of ``<schema>.read_audit_log``, as
# ``pg_get_function_identity_arguments`` renders them; startup compares with it.
READ_AUDIT_LOG_ARGUMENTS: Final = (
    "p_all boolean, p_subjects text[], p_cap integer, p_from timestamp with time zone, "
    "p_to timestamp with time zone, p_subject text, p_table text, p_tables text[], "
    "p_row_key jsonb, p_trace_id text, p_op text, p_after_at timestamp with time zone, "
    "p_after_id bigint, p_ascending boolean, p_limit integer"
)


# The columns of ``<schema>.audit_log`` as ``name type[ not null]``, in order. The
# shape a CDC consumer must also produce; startup and ``verify`` compare with it.
AUDIT_LOG_COLUMNS: Final = (
    "id bigint not null",
    "at timestamp with time zone not null",
    "txid xid8 not null",
    "schema_name text not null",
    "table_name text not null",
    "row_key jsonb not null",
    "op character(1) not null",
    "changed jsonb not null",
    "subject text not null",
    "mechanism text not null",
    "on_behalf_of text not null",
    "elevated text[] not null",
    "scopes jsonb not null",
    "trace_id text not null",
    "db_user name not null",
    "app_name text not null",
    "client_addr inet",
    "backend_pid integer",
    "origin text not null",
)
# Its constraints and indexes, schema-qualified names stripped, sorted. A
# changed primary key, CHECK or index breaks a CDC consumer as much as a column.
AUDIT_LOG_CONSTRAINTS: Final = (
    "constraint CHECK ((op = ANY (ARRAY['I'::bpchar, 'U'::bpchar, 'D'::bpchar])))",
    "constraint CHECK ((origin = ANY (ARRAY['trigger'::text, 'cdc'::text])))",
    "constraint PRIMARY KEY (at, id)",
)
AUDIT_LOG_INDEXES: Final = (
    "index CREATE INDEX audit_log_row_idx ON ONLY audit_log "
    "USING btree (schema_name, table_name, row_key)",
    "index CREATE INDEX audit_log_scopes_idx ON ONLY audit_log USING gin (scopes)",
    "index CREATE INDEX audit_log_trace_idx ON ONLY audit_log USING btree (trace_id)",
    "index CREATE UNIQUE INDEX audit_log_pkey ON ONLY audit_log USING btree (at, id)",
)
AUDIT_LOG_SHAPE: Final = AUDIT_LOG_COLUMNS + AUDIT_LOG_CONSTRAINTS + AUDIT_LOG_INDEXES
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
        "create_range_partition",
        "detach_partition",
        "audit_log_of",
        "audit_log_rel",
        "audit_trigger_source",
        "prepare_audit_log",
        "protect_audit_log",
        "protect_audited_table",
        "unprotect_audited_table",
        "create_audit_partition",
        "ensure_audit_partitions",
        "utc_month",
        "protect_audit_partition",
        "drop_expired_audit_partition",
        "refuse_audit_log_change",
        "require_audit_log",
        "audit_reader_source",
        "apply_audit_reading",
        "unbounded_tables",
        "scope_of",
        "duplicate_scopes",
        "normal_scope",
        "scope_set",
        "tree_mismatch",
    }
)
# The security definer functions the guard installs in the store schema, owned by
# its owner, by full signature: the audit writer and the audit reader. ``verify``
# reports any other one the owner holds, since it may read the audit log (T12b).
# An overload shares the name, never the signature.
SCHEMA_DEFINERS: Final = frozenset(
    {"write_audit_row()", f"read_audit_log({READ_AUDIT_LOG_ARGUMENTS})"}
)
OWNER_TABLES: Final = frozenset(
    {"config", "scoped_table", "scoped_policy", "audited_table", "audit_partition"}
)
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
