"""Elevation wired to the product's declaration and to the active SQLAlchemy session."""

from __future__ import annotations

from collections.abc import Mapping

from loom.core.authz import Decision, RoleCatalog, Scope
from loom.core.authz.elevation import elevate_scope
from loom.core.authz.product import AuthzProduct, load_authz_product
from loom.core.config import ConfigError
from loom.core.model.scoped import ScopedTable
from loom.core.repository.sqlalchemy.session_settings import settings_statement
from loom.core.repository.sqlalchemy.transactional import get_active_session

SCOPE_PREFIX = "loom.scope."


class SQLAlchemyElevationSink:
    """Sets and clears elevation flags on the transaction of the active session."""

    def in_transaction(self) -> bool:
        session = get_active_session()
        return session is not None and session.in_transaction()

    async def set_flag(self, scope: str) -> None:
        await self._apply({f"{SCOPE_PREFIX}{scope}.any": "on"})

    async def clear_flags(self, scopes: frozenset[str]) -> None:
        await self._apply({f"{SCOPE_PREFIX}{scope}.any": "" for scope in sorted(scopes)})

    @staticmethod
    async def _apply(values: Mapping[str, str]) -> None:
        session = get_active_session()
        statement = settings_statement(values)
        if session is None or statement is None:
            return
        clause, params = statement
        await session.execute(clause, params)


async def elevate(
    scope: str, decision: Decision, *, at: Scope, catalog: RoleCatalog | None = None
) -> None:
    """Elevate ``scope`` for the current execution after checking ``decision``.

    Raises:
        RuntimeError: Outside an execution frame.
        ConfigError: Without a product, or when the product did not map ``scope``.
        PermissionError: When the decision does not justify the elevation.
    """
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
