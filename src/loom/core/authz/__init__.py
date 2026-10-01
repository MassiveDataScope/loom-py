"""Role-based authorization primitives, free of any product vocabulary.

A product declares its permissions and roles in code, stores grants (who
holds which role on which scope) wherever it likes, and asks the pure
functions here for decisions.  Scopes are paths whose meaning the product
chooses; a grant on a scope reaches everything below it.
"""

from loom.core.authz._decide import (
    Decision,
    GrantCheck,
    can_grant,
    can_revoke,
    evaluate,
    scopes_with,
)
from loom.core.authz._grants import Grant, GrantSource, InMemoryGrantSource
from loom.core.authz._roles import (
    CompositionRules,
    Permission,
    Role,
    RoleCatalog,
    RoleSpec,
    UnknownPermission,
    UnknownRole,
)
from loom.core.authz._scope import Scope

__all__ = [
    "CompositionRules",
    "Decision",
    "Grant",
    "GrantCheck",
    "GrantSource",
    "InMemoryGrantSource",
    "Permission",
    "Role",
    "RoleCatalog",
    "RoleSpec",
    "Scope",
    "UnknownPermission",
    "UnknownRole",
    "can_grant",
    "can_revoke",
    "evaluate",
    "scopes_with",
]
