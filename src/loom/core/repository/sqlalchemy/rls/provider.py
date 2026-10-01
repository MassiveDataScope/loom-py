"""The session-settings provider loom composes for a scoped application.

It never returns ``None`` and emits every declared key in every transaction:
a value or the empty string for each scope, ``on`` or the empty string for
each elevation flag. A residue left on a pooled connection is therefore always
overwritten, and ``install_pool_reset`` clears it anyway when the connection
returns to the pool.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine

from loom.core.repository.sqlalchemy.rls.sources import resolve_binding, validate_bindings
from loom.core.repository.sqlalchemy.session_settings import SessionSettings

if TYPE_CHECKING:
    from loom.core.locator import Application

SCOPE_PREFIX = "loom.scope."


def rls_session_settings(
    application: Application,
    product: SessionSettings | None = None,
    *,
    elevated: Callable[[], frozenset[str]] = lambda: frozenset(),
) -> Callable[[], Mapping[str, str]]:
    """Build the provider for ``application``; the product's settings are merged in.

    Raises:
        ConfigError: When a scope binding is malformed or unregistered.
        ValueError: At call time, when the product emits a key under ``loom.scope.``.
    """
    validate_bindings(application.scope_sources)
    scopes = _declared_scopes(application)

    def provide() -> Mapping[str, str]:
        active = elevated()
        settings: dict[str, str] = {}
        for scope, elevable in scopes.items():
            value = resolve_binding(application.scope_sources[scope])
            settings[f"{SCOPE_PREFIX}{scope}"] = "" if value is None else str(value)
            if elevable:
                settings[f"{SCOPE_PREFIX}{scope}.any"] = "on" if scope in active else ""
        settings.update(_product_settings(product))
        return settings

    return provide


def install_pool_reset(engine: AsyncEngine) -> None:
    """Run ``RESET ALL`` whenever a connection returns to the pool.

    The open transaction is rolled back first and the reset is committed, because
    Postgres reverts session-level settings changed inside a transaction that is
    later rolled back, which is exactly what the pool does on return.
    """

    @event.listens_for(engine.sync_engine, "reset")
    def _reset(dbapi_connection: Any, _record: Any, _state: Any) -> None:
        dbapi_connection.rollback()
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("RESET ALL")
        finally:
            cursor.close()
        dbapi_connection.commit()


def _declared_scopes(application: Application) -> dict[str, bool]:
    scopes: dict[str, bool] = {}
    for table in application.scoped.values():
        for column in table.scopes:
            scopes[column.scope] = scopes.get(column.scope, False) or column.elevable
    return scopes


def _product_settings(product: SessionSettings | None) -> Mapping[str, str]:
    values = product() if product is not None else None
    if not values:
        return {}
    intruding = sorted(key for key in values if key.startswith(SCOPE_PREFIX))
    if intruding:
        raise ValueError(f"product settings may not use the {SCOPE_PREFIX} namespace: {intruding}")
    return values
