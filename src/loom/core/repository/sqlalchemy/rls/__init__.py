"""Row-level security support for the SQLAlchemy backend.

The product declares its database users, names and schema here; loom installs
a static guard and configures it from that declaration as bound data, and
never names anything itself.
"""

from __future__ import annotations

from loom.core.repository.sqlalchemy.rls.bootstrap import (
    MIN_SERVER_VERSION_NUM,
    apply_bootstrap,
)
from loom.core.repository.sqlalchemy.rls.config import (
    BootstrapConfig,
    DatabaseRoles,
    DatabaseUser,
    SchemaNames,
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
    "DatabaseRoles",
    "DatabaseUser",
    "SQLAlchemyElevationSink",
    "SchemaNames",
    "apply_bootstrap",
    "create_schema",
    "elevate",
    "install_pool_reset",
    "register_scope_source",
    "rls_session_settings",
    "validate_elevations",
    "verify",
]
