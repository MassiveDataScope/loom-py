"""Declaration of the database users and schema one application bootstraps."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from loom.core.backend.scoped_ddl import schema_identifier, sql_identifier

Access = Literal["read", "write", "bypass"]


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
    """Everything the bootstrap renders for one application schema; no name has a default."""

    schema: str
    roles: DatabaseRoles
    database_users: Mapping[str, DatabaseUser]
    revoke_public: bool = True

    @property
    def bypass_users(self) -> tuple[str, ...]:
        """The declared users with ``access="bypass"``, in declaration order."""
        return tuple(name for name, spec in self.database_users.items() if spec.access == "bypass")

    def validated(self) -> BootstrapConfig:
        """Return ``self`` after checking every name Postgres will see.

        Raises:
            ValueError: Naming the first identifier Postgres would fold,
                truncate or resolve to something else.
        """
        schema_identifier(self.schema)
        for name in (self.roles.owner, self.roles.migrator, *self.database_users):
            sql_identifier(name)
        return self
