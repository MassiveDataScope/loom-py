"""Elevation wired to the product's declaration and to the active SQLAlchemy session."""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from typing import Final

from sqlalchemy import text

from loom.core.authz import Decision, RoleCatalog, Scope
from loom.core.authz.elevation import elevate_scope, in_execution_frame
from loom.core.authz.product import AuthzProduct, load_authz_product
from loom.core.config import ConfigError
from loom.core.model.scoped import ScopedTable
from loom.core.repository.sqlalchemy.rls.provider import elevation_setting
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.session_settings import SET_SETTINGS, settings_parameters

_SET_SETTINGS: Final = text(SET_SETTINGS)


class SQLAlchemyElevationSink:
    """Sets and clears elevation flags on the transaction of its store's participant.

    The sink only ever touches the session its own store's participant holds
    in the current invocation, so an elevation flag can never be set on
    another store's session. Without that participant it does nothing.

    Every method reads that session when it is called, through
    :attr:`SessionManager.active_session`, and returns the statement left to
    await, which may then run in any task.

    Args:
        session_manager: The session manager of the store whose sessions it
            elevates.

    Raises:
        ScopeOwnershipError: From any method, outside the task that owns the
            invocation while the store is enlisted.
        ForeignSessionError: From any method, when another session manager's
            unit of work holds the store.
    """

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    def in_transaction(self) -> bool:
        session = self._session_manager.active_session
        return session is not None and session.in_transaction()

    def set_flag(self, scope: str) -> Awaitable[object]:
        return self._apply({elevation_setting(scope): "on"})

    def clear_flags(self, scopes: frozenset[str]) -> Awaitable[object]:
        return self._apply({elevation_setting(scope): "" for scope in sorted(scopes)})

    def invalidate(self) -> Awaitable[object]:
        session = self._session_manager.active_session
        return _nothing() if session is None else session.invalidate()

    def _apply(self, values: Mapping[str, str]) -> Awaitable[object]:
        # Read now, in the calling task: the frame may await the statement in another.
        session = self._session_manager.active_session
        parameters = settings_parameters(values)
        if session is None or parameters is None:
            return _nothing()
        return session.execute(_SET_SETTINGS, parameters)


async def _nothing() -> None:
    """The driver call left when there is no transaction to touch."""


async def elevate(
    scope: str, decision: Decision, *, at: Scope, catalog: RoleCatalog | None = None
) -> None:
    """Elevate ``scope`` for the current execution after checking ``decision``.

    Raises:
        RuntimeError: Outside an execution frame.
        ConfigError: Without a product, or when the product did not map ``scope``.
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


def validate_elevations(
    product: AuthzProduct, scoped: Mapping[tuple[str | None, str], ScopedTable]
) -> None:
    """Fail at startup when ``elevations`` names a scope that is unknown or not elevable."""
    elevable = {c.scope for table in scoped.values() for c in table.scopes if c.elevable}
    declared = {c.scope for table in scoped.values() for c in table.scopes}
    for scope in product.elevations:
        if scope not in declared:
            raise ConfigError(f"AuthzProduct.elevations names unknown scope {scope!r}")
        if scope not in elevable:
            raise ConfigError(
                f"AuthzProduct.elevations names scope {scope!r}, which is not elevable"
            )
