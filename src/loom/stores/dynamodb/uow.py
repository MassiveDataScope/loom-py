"""No-op UnitOfWork for the DynamoDB backend.

DynamoDB has no multi-item transaction boundary that maps to the per-UseCase
Unit of Work used by the relational backend: each write is autocommitted by its
own ``PutItem`` / ``DeleteItem`` call inside
:class:`~loom.stores.dynamodb.repository.DynamoDBRepository`. This
Unit of Work therefore satisfies the
:class:`~loom.core.stores.uow.abc.UnitOfWork` protocol with empty ``begin`` /
``commit`` / ``rollback`` so :class:`~loom.core.engine.executor.UseCaseExecutor`
can wrap UseCases uniformly regardless of backend.
"""

from __future__ import annotations

from typing import ClassVar

from loom.core.stores.uow.abc import UnitOfWork


class DynamoDBUnitOfWork:
    """No-op Unit of Work: writes are autocommitted per ``PutItem`` call.

    Implements the transaction-boundary protocol so the executor can drive
    every backend the same way, but performs no work — DynamoDB commits each
    item write on its own.
    """

    is_transactional: ClassVar[bool] = False

    async def begin(self) -> None:
        """No-op: DynamoDB opens no shared transaction."""

    async def commit(self) -> None:
        """No-op: each item write is already autocommitted."""

    async def rollback(self) -> None:
        """No-op: DynamoDB autocommitted writes cannot be rolled back here."""

    async def __aenter__(self) -> DynamoDBUnitOfWork:
        await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()


class DynamoDBUnitOfWorkFactory:
    """Factory that creates fresh :class:`DynamoDBUnitOfWork` instances.

    Register one instance and inject it into
    :class:`~loom.core.engine.executor.UseCaseExecutor` at startup, mirroring
    :class:`~loom.stores.sqlalchemy.uow.SQLAlchemyUnitOfWorkFactory`.
    """

    def create(self) -> UnitOfWork:
        """Return a fresh, not-yet-begun no-op :class:`UnitOfWork`."""
        return DynamoDBUnitOfWork()
