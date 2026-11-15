"""In-memory stand-in for the boto3 low-level ``dynamodb`` client."""

from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError


class FakeClient:
    """In-memory stand-in for a boto3 low-level ``dynamodb`` client.

    Stores items per table in low-level ``AttributeValue`` form (exactly what
    the client API sends and returns), indexed by the single partition-key
    attribute. Honours the ``attribute_not_exists`` / ``attribute_exists``
    conditions used by ``create`` / ``update`` / ``delete`` by raising a real
    :class:`botocore.exceptions.ClientError` with the
    ``ConditionalCheckFailedException`` code, so error mapping is exercised
    end-to-end. ``transact_write_items`` accepts ``Put`` entries only and, as
    DynamoDB does, refuses more than 100 entries or two on one item with
    ``ValidationException``, and cancels the whole transaction with
    ``TransactionCanceledException`` and per-entry ``CancellationReasons``
    when a condition fails. ``transactions`` records the size of every
    committed transaction.
    """

    def __init__(self, key_name: str, table_names: tuple[str, ...] = ("products",)) -> None:
        self._key = key_name
        self.items: dict[str, dict[Any, dict[str, Any]]] = {name: {} for name in table_names}
        self.table_status: dict[str, str] = dict.fromkeys(table_names, "ACTIVE")
        self.transactions: list[int] = []

    def get_item(  # noqa: N803 - boto3 kwarg names
        self, *, TableName: str, Key: dict[str, Any]
    ) -> dict[str, Any]:
        item = self.items[TableName].get(self._pk(Key))
        return {"Item": dict(item)} if item is not None else {}

    def put_item(  # noqa: N803 - boto3 kwarg names
        self,
        *,
        TableName: str,
        Item: dict[str, Any],
        ConditionExpression: str | None = None,
        ExpressionAttributeNames: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        table = self.items[TableName]
        pk = self._pk(Item)
        self._check_condition(ConditionExpression, exists=pk in table)
        table[pk] = dict(Item)
        return {}

    def transact_write_items(  # noqa: N803 - boto3 kwarg names
        self, *, TransactItems: list[dict[str, Any]]
    ) -> dict[str, Any]:
        puts = [entry["Put"] for entry in TransactItems]
        targets = [(put["TableName"], self._pk(put["Item"])) for put in puts]
        if len(puts) > 100 or len(set(targets)) < len(targets):
            raise ClientError(
                {"Error": {"Code": "ValidationException", "Message": "invalid transaction"}},
                "TransactWriteItems",
            )
        reasons = [
            {"Code": "ConditionalCheckFailed"}
            if self._condition_fails(put.get("ConditionExpression"), exists=pk in self.items[table])
            else {"Code": "None"}
            for put, (table, pk) in zip(puts, targets, strict=True)
        ]
        if any(reason["Code"] != "None" for reason in reasons):
            raise ClientError(
                {
                    "Error": {
                        "Code": "TransactionCanceledException",
                        "Message": "Transaction cancelled",
                    },
                    "CancellationReasons": reasons,
                },
                "TransactWriteItems",
            )
        for put, (table, pk) in zip(puts, targets, strict=True):
            self.items[table][pk] = dict(put["Item"])
        self.transactions.append(len(puts))
        return {}

    def delete_item(  # noqa: N803 - boto3 kwarg names
        self,
        *,
        TableName: str,
        Key: dict[str, Any],
        ConditionExpression: str | None = None,
        ExpressionAttributeNames: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        table = self.items[TableName]
        self._check_condition(ConditionExpression, exists=self._pk(Key) in table)
        table.pop(self._pk(Key), None)
        return {}

    def describe_table(self, *, TableName: str) -> dict[str, Any]:  # noqa: N803 - boto3 kwarg names
        if TableName not in self.items:
            raise ClientError(
                {
                    "Error": {
                        "Code": "ResourceNotFoundException",
                        "Message": "Requested resource not found",
                    }
                },
                "DescribeTable",
            )
        return {"Table": {"TableName": TableName, "TableStatus": self.table_status[TableName]}}

    def _pk(self, item: dict[str, Any]) -> Any:
        """Extract the scalar partition-key value from an AttributeValue dict."""
        return next(iter(item[self._key].values()))

    def _check_condition(self, expression: str | None, *, exists: bool) -> None:
        if self._condition_fails(expression, exists=exists):
            raise self._conditional_failure()

    @staticmethod
    def _condition_fails(expression: str | None, *, exists: bool) -> bool:
        if expression is None:
            return False
        if expression.startswith("attribute_not_exists"):
            return exists
        return expression.startswith("attribute_exists") and not exists

    @staticmethod
    def _conditional_failure() -> ClientError:
        return ClientError(
            {
                "Error": {
                    "Code": "ConditionalCheckFailedException",
                    "Message": "The conditional request failed",
                }
            },
            "PutItem",
        )
