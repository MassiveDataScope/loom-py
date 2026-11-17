"""DynamoDB tables on moto: create one, read back what a repository stored, count transactions."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

KEY = "id"


def create_table(client: Any, name: str, key_type: str = "N") -> None:
    """Create the table *name* keyed by ``id`` of *key_type* (``"N"`` or ``"S"``)."""
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": KEY, "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": KEY, "AttributeType": key_type}],
        BillingMode="PAY_PER_REQUEST",
    )


def stored_items(client: Any, table: str) -> dict[str, dict[str, Any]]:
    """Every item of *table* in ``AttributeValue`` form, keyed by the text of its ``id``."""
    pages = client.get_paginator("scan").paginate(TableName=table)
    return {str(next(iter(item[KEY].values()))): item for page in pages for item in page["Items"]}


@contextmanager
def record_transactions(client: Any) -> Iterator[list[int]]:
    """Record the size of every ``TransactWriteItems`` *client* commits, in order."""
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
    client.meta.events.register(before, remember_size)
    client.meta.events.register(after, keep_committed)
    try:
        yield sizes
    finally:
        client.meta.events.unregister(before, remember_size)
        client.meta.events.unregister(after, keep_committed)
