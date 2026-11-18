"""A DynamoDB table on moto: create it, read back what a repository stored, count transactions."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

KEY = "id"


class MotoTable:
    """One table keyed by ``id`` on a moto DynamoDB client.

    Attributes:
        client: The DynamoDB client the table was created on.
        name: The table name.
    """

    def __init__(self, client: Any, name: str) -> None:
        self.client = client
        self.name = name

    @classmethod
    def create(cls, client: Any, name: str, key_type: str = "N") -> MotoTable:
        """Create the table *name* on *client*, keyed by ``id`` of type *key_type* (N or S)."""
        client.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": KEY, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": KEY, "AttributeType": key_type}],
            BillingMode="PAY_PER_REQUEST",
        )
        return cls(client, name)

    def items_by_id(self) -> dict[str, dict[str, Any]]:
        """Every item in ``AttributeValue`` form, keyed by the text of its ``id``."""
        pages = self.client.get_paginator("scan").paginate(TableName=self.name)
        return {
            str(next(iter(item[KEY].values()))): item for page in pages for item in page["Items"]
        }

    @contextmanager
    def recording_transactions(self) -> Iterator[list[int]]:
        """Yield the size of every ``TransactWriteItems`` the client commits, in order."""
        sizes: list[int] = []
        sent: list[int] = []

        def remember_size(params: dict[str, Any], **_: Any) -> None:
            sent.append(len(params["TransactItems"]))

        def keep_committed(parsed: dict[str, Any], **_: Any) -> None:
            size = sent.pop()
            if "Error" not in parsed:
                sizes.append(size)

        before = "before-parameter-build.dynamodb.TransactWriteItems"
        after = "after-call.dynamodb.TransactWriteItems"
        self.client.meta.events.register(before, remember_size)
        self.client.meta.events.register(after, keep_committed)
        try:
            yield sizes
        finally:
            self.client.meta.events.unregister(before, remember_size)
            self.client.meta.events.unregister(after, keep_committed)
