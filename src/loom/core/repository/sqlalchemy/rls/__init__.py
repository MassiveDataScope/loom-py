"""Row-level security support for the SQLAlchemy backend.

The product declares its database users and schema here; loom renders and
applies the guard objects from that declaration and never names anything
itself.
"""

from __future__ import annotations

from loom.core.repository.sqlalchemy.rls.bootstrap import (
    MIN_SERVER_VERSION_NUM,
    apply_bootstrap,
    render_bootstrap,
)
from loom.core.repository.sqlalchemy.rls.config import (
    BootstrapConfig,
    DatabaseRoles,
    DatabaseUser,
)
from loom.core.repository.sqlalchemy.rls.schema import create_schema

__all__ = [
    "MIN_SERVER_VERSION_NUM",
    "BootstrapConfig",
    "DatabaseRoles",
    "DatabaseUser",
    "apply_bootstrap",
    "create_schema",
    "render_bootstrap",
]
