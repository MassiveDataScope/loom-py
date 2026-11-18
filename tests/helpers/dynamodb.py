"""A DynamoDB table on moto: create it, read back what a repository stored, count transactions."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

KEY = "id"


class MotoTable:
    """One table keyed by ``id`` on a moto DynamoDB client.

    Args:
        client: A DynamoDB client under ``mocked_aws``.
        name: The table name.
        key_type: The type of ``id``, ``"N"`` or ``"S"``.
    """

    def __init__(self, client: Any, name: str, key_type: str = "N") -> None:
        self.client = client
        self.name = name
        self._key_type = key_type

    def create(self) -> MotoTable:
        """Create the table and return it."""
        self.client.create_table(
            TableName=self.name,
            KeySchema=[{"AttributeName": KEY, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": KEY, "AttributeType": self._key_type}],
            BillingMode="PAY_PER_REQUEST",
        )
        return self

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
