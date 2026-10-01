"""Declaration of the database users, names and schema one application bootstraps."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal

from loom.core.schema_names import SchemaNames, schema_identifier, sql_identifier

Access = Literal["read", "write", "bypass"]

BYPASS_VERSION_PRIVILEGES: Final = frozenset({"SELECT"})
MAX_GUARD_LENGTH: Final = 58
MAX_VERSION_TABLE_LENGTH: Final = 59
POSTGRES_SCRAM_ITERATIONS: Final = 4096


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
    only be raised.
    """

    schema: str
    roles: DatabaseRoles
    database_users: Mapping[str, DatabaseUser]
    names: SchemaNames
    revoke_public: bool = False
    scram_iterations: int = POSTGRES_SCRAM_ITERATIONS

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
        if self.scram_iterations < POSTGRES_SCRAM_ITERATIONS:
            raise ValueError(
                f"scram_iterations must be at least {POSTGRES_SCRAM_ITERATIONS}, "
                f"got {self.scram_iterations}"
            )
        return self

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
        }
