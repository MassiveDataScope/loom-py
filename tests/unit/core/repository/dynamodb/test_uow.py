"""``DynamoUnitOfWork``: no-op, writes autocommit per item."""

from __future__ import annotations

from loom.stores.dynamodb.uow import DynamoUnitOfWorkFactory


class TestDynamoUnitOfWork:
    def test_declares_it_cannot_roll_back(self) -> None:
        """F03: writes autocommit here, so the cache bump runs at once."""
        assert type(DynamoUnitOfWorkFactory().create()).is_transactional is False
