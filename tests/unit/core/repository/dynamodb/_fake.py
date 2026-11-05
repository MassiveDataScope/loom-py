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
    end-to-end.
    """

    def __init__(self, key_name: str, table_names: tuple[str, ...] = ("products",)) -> None:
        self._key = key_name
        self.items: dict[str, dict[Any, dict[str, Any]]] = {name: {} for name in table_names}
        self.table_status: dict[str, str] = dict.fromkeys(table_names, "ACTIVE")

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
        if expression is None:
            return
        if expression.startswith("attribute_not_exists") and exists:
            raise self._conditional_failure()
        if expression.startswith("attribute_exists") and not exists:
            raise self._conditional_failure()

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
