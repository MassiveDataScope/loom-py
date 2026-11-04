"""The elevation sink the composition builds, over one SQLAlchemy store."""

from __future__ import annotations

from loom.core.model.privilege import READ_WRITE
from loom.core.model.scoped import ScopeColumn, ScopedTable
from loom.core.persistence.elevation import ScopedElevationSink, ScopedTables
from loom.core.repository.sqlalchemy.rls import SQLAlchemyStoreElevation
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class _Declaring(SQLAlchemyStoreElevation):
    """A store elevation whose tables declare ``elevable``, whatever its metadata holds."""

    def __init__(self, manager: SessionManager, elevable: tuple[str, ...]) -> None:
        super().__init__(manager)
        columns = tuple(ScopeColumn(scope, scope, "write", elevable=True) for scope in elevable)
        self._tables = {(None, "elevable"): ScopedTable(None, "elevable", columns, READ_WRITE)}

    def scoped_tables(self) -> ScopedTables:
        return self._tables


def declaring(manager: SessionManager, *elevable: str) -> SQLAlchemyStoreElevation:
    """``manager``'s store elevation, its tables declaring ``elevable``."""
    return _Declaring(manager, elevable)


def sink_for(manager: SessionManager, *elevable: str) -> ScopedElevationSink:
    """The sink of an application whose one store is ``manager``'s, declaring ``elevable``."""
    return ScopedElevationSink({manager.store: declaring(manager, *elevable)})
