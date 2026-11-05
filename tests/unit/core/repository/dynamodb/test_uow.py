"""``DynamoUnitOfWork``: no-op, writes autocommit per item."""

from __future__ import annotations

from loom.core.repository.dynamodb.uow import DynamoUnitOfWorkFactory


def test_declares_it_cannot_roll_back() -> None:
    """F03: writes autocommit here, so the cache bump runs at once."""
    assert type(DynamoUnitOfWorkFactory().create()).transactional is False
