"""Declaration of the database users, names and schema one application bootstraps."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from loom.core.backend.scoped_ddl import schema_identifier, sql_identifier

Access = Literal["read", "write", "bypass"]

BYPASS_VERSION_PRIVILEGES = frozenset({"SELECT"})
MAX_GUARD_LENGTH = 58
MAX_VERSION_TABLE_LENGTH = 59


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
class SchemaNames:
    """The names around one application schema; declared by the product, never defaulted.

    ``loom schema init`` proposes them with :meth:`derived` and writes them into
    the product's configuration, where the product may change any of them.
    """

    guard: str
    readers: str
    writers: str
    version_table: str
    data_version_table: str

    @classmethod
    def derived(cls, schema: str) -> SchemaNames:
        """Propose the conventional names for ``schema``; used only to write the configuration."""
        schema_identifier(schema)
        return cls(
            guard=f"loom_guard_{schema}",
            readers=f"{schema}_readers",
            writers=f"{schema}_writers",
            version_table="alembic_version",
            data_version_table="alembic_version_data",
        )


@dataclass(frozen=True, slots=True)
class BootstrapConfig:
    """Everything the bootstrap applies for one application schema; no name has a default."""

    schema: str
    roles: DatabaseRoles
    database_users: Mapping[str, DatabaseUser]
    names: SchemaNames
    revoke_public: bool = True

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
        schema_identifier(self.schema)
        sql_identifier(self.names.guard, max_length=MAX_GUARD_LENGTH)
        for table in (self.names.version_table, self.names.data_version_table):
            sql_identifier(table, max_length=MAX_VERSION_TABLE_LENGTH)
        roles = (
            self.roles.owner,
            self.roles.migrator,
            self.names.readers,
            self.names.writers,
            *self.database_users,
        )
        for name in roles:
            sql_identifier(name)
        if len(set(roles)) != len(roles):
            raise ValueError(f"database roles must have distinct names, got {sorted(roles)}")
        if self.names.version_table == self.names.data_version_table:
            raise ValueError("the structural and the data version tables must differ")
        return self

    def document(self) -> dict[str, object]:
        """The bound JSON document the guard's ``configure`` consumes."""
        return {
            "app_schema": self.schema,
            "owner": self.roles.owner,
            "migrator": self.roles.migrator,
            "readers": self.names.readers,
            "writers": self.names.writers,
            "users": [
                {"name": name, "login": spec.login, "access": spec.access}
                for name, spec in self.database_users.items()
            ],
            "revoke_public": self.revoke_public,
            "version_table": self.names.version_table,
            "data_version_table": self.names.data_version_table,
        }
