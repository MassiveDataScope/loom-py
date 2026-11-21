"""Row-level security support for the SQLAlchemy backend.

The product declares its database users, names and schema here; loom installs
a static guard and configures it from that declaration as bound data, and
never names anything itself.
"""

from __future__ import annotations

from loom.core.authz.elevation import ReadOnlyElevation
from loom.core.schema_names import SchemaNames
from loom.core.stores.elevation import validate_elevations
from loom.stores.sqlalchemy.rls.bootstrap import (
    MIN_SERVER_VERSION_NUM,
    apply_bootstrap,
)
from loom.stores.sqlalchemy.rls.config import (
    BootstrapConfig,
    DatabaseRoles,
    DatabaseUser,
)
from loom.stores.sqlalchemy.rls.elevate import SQLAlchemyStoreElevation, elevate
from loom.stores.sqlalchemy.rls.findings import Finding
from loom.stores.sqlalchemy.rls.partitions import (
    detach_range_partitions,
    ensure_range_partitions,
)
from loom.stores.sqlalchemy.rls.provider import install_pool_reset, rls_session_settings
from loom.stores.sqlalchemy.rls.schema import create_schema
from loom.stores.sqlalchemy.rls.sources import register_scope_source
from loom.stores.sqlalchemy.rls.verify import Report, verify

__all__ = [
    "MIN_SERVER_VERSION_NUM",
    "BootstrapConfig",
    "Finding",
    "Report",
    "DatabaseRoles",
    "DatabaseUser",
    "ReadOnlyElevation",
    "SQLAlchemyStoreElevation",
    "SchemaNames",
    "apply_bootstrap",
    "create_schema",
    "detach_range_partitions",
    "elevate",
    "ensure_range_partitions",
    "install_pool_reset",
    "register_scope_source",
    "rls_session_settings",
    "validate_elevations",
    "verify",
]
