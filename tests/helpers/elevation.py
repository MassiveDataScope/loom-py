"""The elevation sink the composition builds, over one SQLAlchemy store."""

from __future__ import annotations

from loom.core.persistence.elevation import ScopedElevationSink
from loom.core.repository.sqlalchemy.rls import SQLAlchemyStoreElevation
from loom.core.repository.sqlalchemy.session_manager import SessionManager


def sink_for(manager: SessionManager, *elevable: str) -> ScopedElevationSink:
    """The sink of an application whose one store is ``manager``'s, declaring ``elevable``."""
    return ScopedElevationSink(
        {manager.store: SQLAlchemyStoreElevation(manager)}, {manager.store: frozenset(elevable)}
    )
