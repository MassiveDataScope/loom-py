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
from loom.core.repository.sqlalchemy.rls.elevate import (
    SQLAlchemyElevationSink,
    elevate,
    validate_elevations,
)
from loom.core.repository.sqlalchemy.rls.provider import install_pool_reset, rls_session_settings
from loom.core.repository.sqlalchemy.rls.schema import create_schema
from loom.core.repository.sqlalchemy.rls.sources import register_scope_source
from loom.core.repository.sqlalchemy.rls.verify import Finding, Report, verify

__all__ = [
    "MIN_SERVER_VERSION_NUM",
    "BootstrapConfig",
    "Finding",
    "Report",
    "SQLAlchemyElevationSink",
    "DatabaseRoles",
    "DatabaseUser",
    "apply_bootstrap",
    "create_schema",
    "elevate",
    "install_pool_reset",
    "register_scope_source",
    "render_bootstrap",
    "rls_session_settings",
    "validate_elevations",
    "verify",
]
