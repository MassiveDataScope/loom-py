"""Declaration of the database users, names and schema one application bootstraps."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal

from sqlalchemy.engine import make_url

from loom.core.config import ConfigError
from loom.core.schema_names import SchemaNames, schema_identifier, sql_identifier

if TYPE_CHECKING:
    from sqlalchemy import MetaData
    from sqlalchemy.types import TypeEngine

    from loom.core.model.scoped import ScopedTables

Access = Literal["read", "write", "bypass"]

BYPASS_VERSION_PRIVILEGES: Final = frozenset({"SELECT"})
MAX_GUARD_LENGTH: Final = 58
MAX_VERSION_TABLE_LENGTH: Final = 59
POSTGRES_SCRAM_ITERATIONS: Final = 4096
# The spellings SQLAlchemy's PostgreSQL dialect compiles that ``format_type(t, NULL)`` renders
# otherwise; every other compiled base type is already Postgres' own name in upper case.
_FORMAT_TYPE_NAMES: Final = {
    "varchar": "character varying",
    "char": "character",
    "float": "double precision",
}
_TYPE_MODIFIER: Final = re.compile(r"\([^)]*\)")
# The dialect loom stores use; compiling a type loads no driver.
_POSTGRES: Final = make_url("postgresql+asyncpg://").get_dialect()()


@dataclass(frozen=True, slots=True)
class DatabaseRoles:
    """The two database roles every scoped schema needs; unrelated to RBAC roles."""

    owner: str
    migrator: str


@dataclass(frozen=True, slots=True)
class DatabaseUser:
    """A database user the product declares, with how far it may reach."""

    login: bool
    access: Access


@dataclass(frozen=True, slots=True)
class BootstrapConfig:
    """Everything the bootstrap applies for one application schema; no name has a default.

    ``revoke_public`` revokes every ``PUBLIC`` privilege on the ``public``
    schema; it is opt-in because that schema belongs to the whole database.
    ``scram_iterations`` is the PBKDF2 iteration count of every password
    verifier; it defaults to Postgres' own ``scram_iterations`` (4096) and may
    only be raised. ``auditor`` names the role that reads the audit log
    (``<schema>_auditor`` when not declared) and ``audit_retention_months`` is
    how long its partitions are kept; without one nothing is ever dropped.
    ``audit_reading`` is the store's ``schema.audit.reading.enabled``;
    ``audit_boundary`` is the boundary ``B`` (scope name and Postgres base
    type) derived while reading is on, and ``clear_audit_boundary`` asks
    ``configure`` to forget a recorded ``B``, which it allows with reading off.
    """

    schema: str
    roles: DatabaseRoles
    database_users: Mapping[str, DatabaseUser]
    names: SchemaNames
    revoke_public: bool = False
    scram_iterations: int = POSTGRES_SCRAM_ITERATIONS
    auditor: str | None = None
    audit_retention_months: int | None = None
    audit_reading: bool = False
    audit_boundary: tuple[str, str] | None = None
    clear_audit_boundary: bool = False

    @property
    def auditor_role(self) -> str:
        """The role that reads the audit log: the declared one or ``<schema>_auditor``."""
        return self.auditor or f"{self.schema}_auditor"

    @property
    def bypass_users(self) -> tuple[str, ...]:
        """The declared users with ``access="bypass"``, in declaration order."""
        return tuple(name for name, spec in self.database_users.items() if spec.access == "bypass")

    def validated(self) -> BootstrapConfig:
        """Return ``self`` after checking every name Postgres will see.

        Raises:
            ValueError: Naming the first identifier Postgres would fold,
                truncate or resolve to something else, or two roles sharing a name.
        """
        self._check_names()
        self._check_settings()
        return self

    def _check_names(self) -> None:
        """Every identifier is one Postgres keeps as written, and the roles are distinct."""
        schema_identifier(self.schema)
        sql_identifier(self.names.guard, max_length=MAX_GUARD_LENGTH)
        for table in (self.names.version_table, self.names.data_version_table):
            sql_identifier(table, max_length=MAX_VERSION_TABLE_LENGTH)
        roles = (
            self.roles.owner,
            self.roles.migrator,
            self.names.readers,
            self.names.writers,
            self.auditor_role,
            *self.database_users,
        )
        for name in roles:
            sql_identifier(name)
        if len(set(roles)) != len(roles):
            raise ValueError(f"database roles must have distinct names, got {sorted(roles)}")

    def _check_settings(self) -> None:
        """The retention, the version tables and the SCRAM iterations are usable."""
        if self.audit_retention_months is not None and self.audit_retention_months < 1:
            raise ValueError(
                f"audit retention_months must be at least 1, got {self.audit_retention_months}"
            )
        if self.names.version_table == self.names.data_version_table:
            raise ValueError("the structural and the data version tables must differ")
        if self.scram_iterations < POSTGRES_SCRAM_ITERATIONS:
            raise ValueError(
                f"scram_iterations must be at least {POSTGRES_SCRAM_ITERATIONS}, "
                f"got {self.scram_iterations}"
            )

    def document(self) -> dict[str, object]:
        """The bound JSON document the guard's ``configure`` consumes."""
        return {
            "app_schema": self.schema,
            "owner_role": self.roles.owner,
            "migrator_role": self.roles.migrator,
            "readers_role": self.names.readers,
            "writers_role": self.names.writers,
            "users": [
                {"name": name, "login": spec.login, "access": spec.access}
                for name, spec in self.database_users.items()
            ],
            "revoke_public": self.revoke_public,
            "version_table": self.names.version_table,
            "data_version_table": self.names.data_version_table,
            "auditor_role": self.auditor_role,
            "retention_months": self.audit_retention_months,
            "audit_reading": self.audit_reading,
            "boundary_scope": self.audit_boundary[0] if self.audit_boundary else None,
            "boundary_type": self.audit_boundary[1] if self.audit_boundary else None,
            "clear_audit_boundary": self.clear_audit_boundary,
        }


def derive_audit_boundary(
    scoped: ScopedTables, metadata: MetaData, *, path: str
) -> tuple[str, str] | None:
    """The one boundary ``B`` every scoped table shares: its scope name and Postgres base type.

    The type is the boundary column's type compiled by the PostgreSQL dialect
    without its modifier, as ``format_type(t, NULL)`` renders it, so
    ``varchar(36)`` and ``varchar(64)`` are one base type.

    Returns:
        ``None`` when *scoped* is empty.

    Raises:
        ConfigError: Naming ``<path>.schema.audit.reading`` and the tables of
            each boundary when they use more than one name or base type (FR-012).
    """
    tables: dict[tuple[str, str], list[str]] = {}
    for (schema, name), table in sorted(scoped.items(), key=lambda item: item[0][1]):
        boundary = table.boundary
        column = metadata.tables[f"{schema}.{name}" if schema else name].c[boundary.column]
        tables.setdefault((boundary.scope, _base_type(column.type)), []).append(name)
    if len(tables) > 1:
        found = "; ".join(
            f"{scope} ({type_name}) on {', '.join(names)}"
            for (scope, type_name), names in sorted(tables.items())
        )
        raise ConfigError(
            f"{path}.schema.audit.reading needs one boundary scope and column type on every "
            f"scoped table; found {found}"
        )
    return next(iter(tables), None)


def _base_type(column_type: TypeEngine[object]) -> str:
    """*column_type* compiled for PostgreSQL, without modifier, in ``format_type`` spelling."""
    compiled = column_type.compile(dialect=_POSTGRES)
    base = " ".join(_TYPE_MODIFIER.sub("", compiled).lower().split())
    return _FORMAT_TYPE_NAMES.get(base, base)
