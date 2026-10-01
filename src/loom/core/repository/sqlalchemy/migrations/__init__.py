"""Alembic support for row-scoped schemas: the hook, the environment and the runners.

``ENV_TEMPLATE_PATH`` points at the environment a product copies as ``env.py``
into each tree; it is not importable outside an Alembic run.
"""

from __future__ import annotations

from pathlib import Path

from loom.core.repository.sqlalchemy.migrations.hook import scope_protection_hook
from loom.core.repository.sqlalchemy.migrations.runners import (
    alembic_config,
    check,
    run_data_migrations,
    run_migrations,
)

ENV_TEMPLATE_PATH = Path(__file__).with_name("env_template.py")

__all__ = [
    "ENV_TEMPLATE_PATH",
    "alembic_config",
    "check",
    "run_data_migrations",
    "run_migrations",
    "scope_protection_hook",
]
