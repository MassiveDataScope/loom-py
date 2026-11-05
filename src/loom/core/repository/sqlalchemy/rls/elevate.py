"""Elevation wired to the product's declaration and to the active SQLAlchemy session."""

from __future__ import annotations

from collections.abc import Awaitable
from typing import Final, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.authz import Decision, RoleCatalog, Scope
from loom.core.authz.elevation import elevate_scope, in_execution_frame
from loom.core.authz.product import load_authz_product
from loom.core.backend.sqlalchemy import scoped_tables
from loom.core.config import ConfigError
from loom.core.persistence.elevation import ScopedTables
from loom.core.repository.sqlalchemy.rls.provider import elevation_setting
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.session_settings import SET_SETTINGS, settings_parameters

_SET_SETTINGS: Final = text(SET_SETTINGS)


class SQLAlchemyStoreElevation:
    """Sets and clears elevation flags on the transaction of its store's participant.

    The :class:`~loom.core.persistence.elevation.StoreElevation` of a
    SQLAlchemy store. It reads only the session its own store's participant
    holds, through :attr:`SessionManager.active_session`, so a flag never
    reaches another store's session; the statement it returns may be awaited
    in any task.

    Raises:
        ScopeOwnershipError: From any method, outside the task that owns the
            invocation while the store is enlisted.
        ForeignSessionError: From any method, when another session manager's
            unit of work holds the store.
    """

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    def scoped_tables(self) -> ScopedTables:
        return scoped_tables(self._session_manager.metadata)

    def in_transaction(self) -> bool:
        session = self._session_manager.active_session
        return session is not None and session.in_transaction()

    def set_flag(self, scope: str) -> Awaitable[object]:
        return self._open_session().execute(
            _SET_SETTINGS, settings_parameters({elevation_setting(scope): "on"})
        )

    def clear_flags(self, scopes: frozenset[str]) -> Awaitable[object]:
        values = {elevation_setting(scope): "" for scope in sorted(scopes)}
        return self._open_session().execute(_SET_SETTINGS, settings_parameters(values))

    def invalidate(self) -> Awaitable[object]:
        return self._open_session().invalidate()

    def _open_session(self) -> AsyncSession:
        # Called only while in_transaction() holds, so the participant holds one.
        return cast("AsyncSession", self._session_manager.active_session)


async def elevate(
    scope: str, decision: Decision, *, at: Scope, catalog: RoleCatalog | None = None
) -> None:
    """Elevate ``scope`` for the current execution after checking ``decision``.

    Raises:
        RuntimeError: Outside an execution frame.
        ConfigError: Without a product, or when the product did not map ``scope``.
        ReadOnlyElevation: In a read-only execution.
        PermissionError: When the decision does not justify the elevation.
    """
    if not in_execution_frame():
        raise RuntimeError("elevate requires an open execution frame")
    product = load_authz_product()
    if product is None:
        raise ConfigError("no authorization product is registered under the loom.authz entry point")
    permission = product.elevations.get(scope)
    if permission is None:
        raise ConfigError(f"scope {scope!r} is not declared in AuthzProduct.elevations")
    await elevate_scope(
        scope, decision, at=at, permission=permission, catalog=catalog or product.catalog
    )
