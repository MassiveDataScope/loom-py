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

from loom.core.authz.elevation import elevated_scopes
from loom.core.config import ConfigError
from loom.core.model.scoped import ScopedTable
from loom.core.repository.sqlalchemy.rls.sources import resolve_binding, validate_bindings
from loom.core.repository.sqlalchemy.session_settings import SessionSettings

if TYPE_CHECKING:
    from loom.core.locator import Application

SCOPE_PREFIX = "loom.scope."


def scope_setting(scope: str) -> str:
    """The session key that carries the value of ``scope``."""
    return f"{SCOPE_PREFIX}{scope}"


def elevation_setting(scope: str) -> str:
    """The session key that flags ``scope`` as elevated."""
    return f"{SCOPE_PREFIX}{scope}.any"


def rls_session_settings(
    application: Application, product: SessionSettings | None = None
) -> Callable[[], Mapping[str, str]]:
    """Build the provider for ``application``; the product's settings are merged in.

    Raises:
        ConfigError: When a scope binding is missing, malformed or unregistered.
        ValueError: At call time, when the product emits a key under ``loom.scope.``.
    """
    return scoped_session_settings(application.scope_sources, application.scoped, product)


def scoped_session_settings(
    scope_sources: Mapping[str, str],
    scoped: Mapping[tuple[str | None, str], ScopedTable],
    product: SessionSettings | None = None,
) -> Callable[[], Mapping[str, str]]:
    """Build the provider from the scope bindings and the compiled scoped tables.

    Raises:
        ConfigError: When a declared scope has no binding, or a binding is
            malformed or unregistered.
    """
    scopes = _declared_scopes(scoped)
    missing = sorted(scope for scope in scopes if scope not in scope_sources)
    if missing:
        raise ConfigError(f"database.schema.scopes is missing bindings for {missing}")
    validate_bindings({scope: scope_sources[scope] for scope in scopes})
    return _ScopedProvider(scopes, scope_sources, product)


class _ScopedProvider:
    def __init__(
        self,
        scopes: Mapping[str, bool],
        scope_sources: Mapping[str, str],
        product: SessionSettings | None,
    ) -> None:
        self._scopes = dict(scopes)
        self._sources = dict(scope_sources)
        self._product = product

    def __call__(self) -> Mapping[str, str]:
        active = elevated_scopes()
        settings = dict(_product_settings(self._product))
        for scope, elevable in self._scopes.items():
            value = resolve_binding(self._sources[scope])
            settings[scope_setting(scope)] = "" if value is None else str(value)
            if elevable:
                settings[elevation_setting(scope)] = "on" if scope in active else ""
        return settings


class DeferredScopedSettings:
    """A provider installed when the engine is built and bound at startup.

    The backend builds its engine before the models are compiled, so the
    scoped tables are only known at startup; until then any use fails.
    """

    def __init__(self, scope_sources: Mapping[str, str]) -> None:
        self._scope_sources = dict(scope_sources)
        self._provide: Callable[[], Mapping[str, str]] | None = None

    def bind(self, scoped: Mapping[tuple[str | None, str], ScopedTable]) -> None:
        """Build the provider for the compiled scoped tables; validates every binding."""
        self._provide = scoped_session_settings(self._scope_sources, scoped)

    def __call__(self) -> Mapping[str, str]:
        if self._provide is None:
            raise RuntimeError("scoped session settings were used before startup bound them")
        return self._provide()


def install_pool_reset(engine: AsyncEngine) -> None:
    """Run ``RESET ALL`` whenever a connection returns to the pool.

    The open transaction is rolled back first and the reset is committed, because
    Postgres reverts session-level settings changed inside a transaction that is
    later rolled back, which is exactly what the pool does on return.
    """

    @event.listens_for(engine.sync_engine, "reset")
    def _reset(dbapi_connection: Any, _record: Any, state: Any) -> None:
        if state.terminate_only or not state.asyncio_safe:
            return
        dbapi_connection.rollback()
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("RESET ALL")
        finally:
            cursor.close()
        dbapi_connection.commit()


def _declared_scopes(scoped: Mapping[tuple[str | None, str], ScopedTable]) -> dict[str, bool]:
    scopes: dict[str, bool] = {}
    for table in scoped.values():
        for column in table.scopes:
            scopes[column.scope] = scopes.get(column.scope, False) or column.elevable
    return scopes


def _product_settings(product: SessionSettings | None) -> Mapping[str, str]:
    values = product() if product is not None else None
    if not values:
        return {}
    intruding = sorted(key for key in values if key.lower().startswith(SCOPE_PREFIX))
    if intruding:
        raise ValueError(f"product settings may not use the {SCOPE_PREFIX} namespace: {intruding}")
    return values
