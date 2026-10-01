"""Declaration of the database users and schema one application bootstraps."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

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
