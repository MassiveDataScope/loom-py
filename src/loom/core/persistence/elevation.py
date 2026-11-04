"""Elevation across stores: each elevable scope belongs to one store (ADR 0008, D2).

An elevation flag is set on the transaction of the store whose tables declare
its scope, and on no other. :func:`validate_elevations` refuses at startup a
scope that would make that store ambiguous, and :class:`ScopedElevationSink`
routes the frame's requests to the store that declares each scope.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Mapping
from enum import unique
from typing import Protocol

from loom.core.authz.product import AuthzProduct, load_authz_product
from loom.core.config import ConfigError
from loom.core.errors.codes import CompileIssue, IssueCode, Template
from loom.core.model.scoped import ScopedTable
from loom.core.persistence.stores import StoreKey

ScopedTables = Mapping[tuple[str | None, str], ScopedTable]
"""One store's compiled scoped tables, by schema and name."""


@unique
class ElevationErrorCode(IssueCode):
    """Catalogue of the elevation failures startup reports and the issue each renders."""

    ELEVABLE_SCOPE_IN_SEVERAL_STORES = (
        "ELEVABLE_SCOPE_IN_SEVERAL_STORES",
        Template(
            "scope {scope} is elevable or named in AuthzProduct.elevations, and stores "
            "{stores} all declare it; an elevation must reach one store only. Rename the "
            "scope in all but one store.",
            field="{scope}",
            component="AuthzProduct.elevations",
        ),
    )


class ElevableScopeInSeveralStores(ConfigError):
    """Raised at startup when an elevated scope is declared by two or more stores.

    Attributes:
        issue: The typed issue: code, field (the scope) and message.
    """

    def __init__(self, issue: CompileIssue[ElevationErrorCode]) -> None:
        super().__init__(issue.message)
        self.issue = issue


class StoreElevation(Protocol):
    """How one store's open transaction gets its elevation flags.

    Each method reads the store's session when it is called, through the
    invocation scope's checkpoint, and returns the driver call left to await.
    """

    def scoped_tables(self) -> ScopedTables:
        """The store's compiled scoped tables; known once its models are prepared."""
        ...

    def in_transaction(self) -> bool:
        """Whether the store's participant has a transaction open."""
        ...

    def set_flag(self, scope: str) -> Awaitable[object]:
        """Flag ``scope`` on the open transaction."""
        ...

    def clear_flags(self, scopes: frozenset[str]) -> Awaitable[object]:
        """Clear ``scopes`` on the open transaction."""
        ...

    def invalidate(self, cause: BaseException) -> Awaitable[object]:
        """Forbid the invocation to commit, for ``cause``, and invalidate the open session."""
        ...


class ScopedElevationSink:
    """The elevation sink of an application: each flag goes to the store declaring its scope.

    Implements :class:`~loom.core.authz.elevation.ElevationSink`. A store is
    touched only when it declares one of the scopes asked about and has a
    transaction open, and each check reads its session through the
    invocation scope's checkpoint: a task other than the invocation's owner
    gets ``ScopeOwnershipError``.

    Args:
        stores: How each store's open transaction gets its flags.
        elevable: The elevable scopes of each store in ``stores``.
    """

    def __init__(
        self, stores: Mapping[StoreKey, StoreElevation], elevable: Mapping[StoreKey, frozenset[str]]
    ) -> None:
        self._stores = stores
        self._elevable = elevable

    def in_transaction(self, scopes: frozenset[str]) -> bool:
        return any(store.in_transaction() for store, _ in self._declaring(scopes))

    # Each store's call is made here, in the calling task, where the checkpoint
    # passes: only the awaiting may happen in another task.
    def set_flag(self, scope: str) -> Awaitable[object]:
        return _together([store.set_flag(scope) for store, _ in self._open(frozenset({scope}))])

    def clear_flags(self, scopes: frozenset[str]) -> Awaitable[object]:
        return _together([store.clear_flags(own) for store, own in self._open(scopes)])

    def invalidate(self, scopes: frozenset[str], cause: BaseException) -> Awaitable[object]:
        return _together([store.invalidate(cause) for store, _ in self._open(scopes)])

    def _declaring(self, scopes: frozenset[str]) -> list[tuple[StoreElevation, frozenset[str]]]:
        """Each store declaring some of ``scopes``, with the ones it declares."""
        return [
            (self._stores[key], scopes & elevable)
            for key, elevable in self._elevable.items()
            if scopes & elevable
        ]

    def _open(self, scopes: frozenset[str]) -> list[tuple[StoreElevation, frozenset[str]]]:
        """:meth:`_declaring`, keeping the stores with a transaction open."""
        return [(store, own) for store, own in self._declaring(scopes) if store.in_transaction()]


async def _together(steps: list[Awaitable[object]]) -> None:
    """Await every store's driver call; the first failure is raised once all have finished."""
    await asyncio.gather(*steps)


def elevable_scopes(scoped: ScopedTables) -> frozenset[str]:
    """The scopes ``scoped`` declares elevable."""
    return frozenset(c.scope for table in scoped.values() for c in table.scopes if c.elevable)


def validate_elevations(product: AuthzProduct, scoped: Mapping[StoreKey, ScopedTables]) -> None:
    """Fail at startup when an elevation cannot reach exactly one store.

    Checks the union of every store's scoped tables. A scope named in
    ``product.elevations`` must be declared and elevable. A scope that is
    elevable, or named there, must be declared by one store only; a shared
    scope that is never elevated stays allowed.

    Args:
        product: The registered authorization product.
        scoped: Each store's compiled scoped tables.

    Raises:
        ConfigError: When ``product.elevations`` names an unknown scope, or one
            no store declares elevable.
        ElevableScopeInSeveralStores: When an elevated scope is declared by
            two or more stores.
    """
    declared = {key: _declared_scopes(tables) for key, tables in scoped.items()}
    elevable = frozenset().union(*(elevable_scopes(tables) for tables in scoped.values()))
    for scope in product.elevations:
        if not any(scope in scopes for scopes in declared.values()):
            raise ConfigError(f"AuthzProduct.elevations names unknown scope {scope!r}")
        if scope not in elevable:
            raise ConfigError(
                f"AuthzProduct.elevations names scope {scope!r}, which is not elevable"
            )
    for scope in sorted(elevable | product.elevations.keys()):
        stores = sorted(key for key, scopes in declared.items() if scope in scopes)
        if len(stores) > 1:
            issue = ElevationErrorCode.ELEVABLE_SCOPE_IN_SEVERAL_STORES.issue(
                scope=scope, stores=stores
            )
            raise ElevableScopeInSeveralStores(issue)


def scoped_elevation_sink(stores: Mapping[StoreKey, StoreElevation]) -> ScopedElevationSink:
    """Check the registered product's elevations against ``stores``, then build their sink.

    Call it once every store's models are prepared.

    Raises:
        ConfigError: See :func:`validate_elevations`.
    """
    scoped = {key: store.scoped_tables() for key, store in stores.items()}
    product = load_authz_product()
    if product is not None:
        validate_elevations(product, scoped)
    return ScopedElevationSink(
        stores, {key: elevable_scopes(tables) for key, tables in scoped.items()}
    )


def _declared_scopes(scoped: ScopedTables) -> frozenset[str]:
    return frozenset(c.scope for table in scoped.values() for c in table.scopes)


__all__ = [
    "ElevableScopeInSeveralStores",
    "ElevationErrorCode",
    "ScopedElevationSink",
    "StoreElevation",
    "elevable_scopes",
    "scoped_elevation_sink",
    "validate_elevations",
]
