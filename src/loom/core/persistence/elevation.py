"""Elevation across stores: each elevable scope belongs to one store (ADR 0008, D2).

An elevation flag is set on the transaction of the store whose tables declare
its scope, and on no other. :func:`validate_elevations` refuses at startup a
scope that would make that store ambiguous, and :class:`ScopedElevationSink`
routes the frame's requests to the store that declares each scope.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Iterable, Mapping
from enum import unique
from typing import Protocol

from loom.core.authz.product import AuthzProduct, load_authz_product
from loom.core.config import ConfigError
from loom.core.errors.codes import CompileIssue, IssueCode, Template
from loom.core.model.scoped import ScopedTables, declared_scopes
from loom.core.persistence.stores import StoreKey
from loom.core.uow.scope import current_scope


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
    The flag methods are called only while :meth:`in_transaction` is true.
    """

    def scoped_tables(self) -> ScopedTables:
        """The store's compiled scoped tables; known once its models are prepared."""
        ...

    def in_transaction(self) -> bool: ...

    def set_flag(self, scope: str) -> Awaitable[object]: ...

    def clear_flags(self, scopes: frozenset[str]) -> Awaitable[object]: ...

    def invalidate(self) -> Awaitable[object]:
        """Drop the open session: the flags it may hold on go with it."""
        ...


class ScopedElevationSink:
    """The elevation sink of an application: each flag goes to the store declaring its scope.

    Implements :class:`~loom.core.authz.elevation.ElevationSink`. A store is
    touched only when it declares one of the scopes asked about and has a
    transaction open, and each check reads its session through the
    invocation scope's checkpoint: a task other than the invocation's owner
    gets ``ScopeOwnershipError``. An invalidation first forbids each store's
    invocation to commit. Build it once every store's models are prepared:
    it reads each store's elevable scopes then, and checks them, and the
    registered product's elevations if any, with :func:`validate_elevations`.

    Args:
        stores: How each store's open transaction gets its flags.

    Raises:
        ConfigError: See :func:`validate_elevations`.
    """

    def __init__(self, stores: Mapping[StoreKey, StoreElevation]) -> None:
        scoped = {key: store.scoped_tables() for key, store in stores.items()}
        validate_elevations(load_authz_product(), scoped)
        self._stores = stores
        self._elevable = {key: elevable_scopes(tables) for key, tables in scoped.items()}

    def in_transaction(self, scopes: frozenset[str]) -> bool:
        return any(
            self._stores[k].in_transaction() for k, e in self._elevable.items() if scopes & e
        )

    # Each store's call is made here, in the calling task, where the checkpoint
    # passes: only the awaiting may happen in another task.
    def set_flag(self, scope: str) -> Awaitable[object]:
        return _together(self._stores[k].set_flag(scope) for k in self._open(frozenset({scope})))

    def clear_flags(self, scopes: frozenset[str]) -> Awaitable[object]:
        return _together(self._stores[k].clear_flags(o) for k, o in self._open(scopes).items())

    def invalidate(self, scopes: frozenset[str], cause: BaseException) -> Awaitable[object]:
        open_stores = self._open(scopes)
        invocation = current_scope()
        if invocation is not None:  # each store poisoned before any session is touched
            invocation.poison(open_stores, cause)
        return _together(self._stores[key].invalidate() for key in open_stores)

    def _open(self, scopes: frozenset[str]) -> dict[StoreKey, frozenset[str]]:
        """Each store declaring some of ``scopes`` with a transaction open: the ones it declares."""
        return {
            key: scopes & elevable
            for key, elevable in self._elevable.items()
            if scopes & elevable and self._stores[key].in_transaction()
        }


def _together(steps: Iterable[Awaitable[object]]) -> Awaitable[None]:
    """Make every store's driver call now, in the calling task, and await them in turn.

    When a call raises here, the calls already made are closed unawaited
    before the error propagates.
    """
    made: list[Awaitable[object]] = []
    try:
        for step in steps:
            made.append(step)
    except BaseException:
        _close(made)
        raise
    return _in_turn(made)


async def _in_turn(steps: list[Awaitable[object]]) -> None:
    """Await each step after the previous one; the first failure is raised once all have run."""
    failure: Exception | None = None
    pending = iter(steps)
    try:
        for step in pending:
            try:
                await step
            except Exception as exc:
                failure = failure or exc
    finally:
        _close(pending)  # what an interruption left unawaited
    if failure is not None:
        raise failure


def _close(steps: Iterable[Awaitable[object]]) -> None:
    for step in steps:
        if inspect.iscoroutine(step):
            step.close()


def elevable_scopes(scoped: ScopedTables) -> frozenset[str]:
    """The scopes ``scoped`` declares elevable."""
    return frozenset(scope for scope, elevable in declared_scopes(scoped).items() if elevable)


def validate_elevations(
    product: AuthzProduct | None, scoped: Mapping[StoreKey, ScopedTables]
) -> None:
    """Fail at startup when an elevation cannot reach exactly one store.

    Checks the union of every store's scoped tables. A scope named in
    ``product.elevations`` must be declared and elevable. A scope that is
    elevable, or named there, must be declared by one store only; a shared
    scope that is never elevated stays allowed. Without a product, only
    that last rule applies.

    Args:
        product: The registered authorization product, if any.
        scoped: Each store's compiled scoped tables.

    Raises:
        ConfigError: When ``product.elevations`` names an unknown scope, or one
            no store declares elevable.
        ElevableScopeInSeveralStores: When an elevated scope is declared by
            two or more stores.
    """
    declared = {key: declared_scopes(tables).keys() for key, tables in scoped.items()}
    elevable = frozenset().union(*(elevable_scopes(tables) for tables in scoped.values()))
    elevations = {} if product is None else product.elevations
    for scope in elevations:
        if not any(scope in scopes for scopes in declared.values()):
            raise ConfigError(f"AuthzProduct.elevations names unknown scope {scope!r}")
        if scope not in elevable:
            raise ConfigError(
                f"AuthzProduct.elevations names scope {scope!r}, which is not elevable"
            )
    for scope in sorted(elevable | elevations.keys()):
        stores = sorted(key for key, scopes in declared.items() if scope in scopes)
        if len(stores) > 1:
            issue = ElevationErrorCode.ELEVABLE_SCOPE_IN_SEVERAL_STORES.issue(
                scope=scope, stores=stores
            )
            raise ElevableScopeInSeveralStores(issue)


__all__ = [
    "ElevableScopeInSeveralStores",
    "ElevationErrorCode",
    "ScopedElevationSink",
    "ScopedTables",
    "StoreElevation",
    "elevable_scopes",
    "validate_elevations",
]
