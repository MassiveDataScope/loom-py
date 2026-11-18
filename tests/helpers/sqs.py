"""A fake boto3 ``sqs`` client that records every call, for the queue and settlement tests."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from tests.helpers.aws import ACCOUNT

URL_BASE = f"https://sqs.eu-west-1.amazonaws.com/{ACCOUNT}"


def queue_url(name: str) -> str:
    """The URL of the queue *name* in the test account."""
    return f"{URL_BASE}/{name}"


class FakeSqs:
    """Stands in for the ``sqs`` client.

    Attributes:
        calls: How many times each API method was called.
        batches: Each batch call as its method, queue URL and entries.
        sent: Each ``SendMessage`` request.

    Args:
        redrive: The ``RedrivePolicy`` of each queue URL; a queue absent has none.
        refuse: Entry ids a batch answers as failed, with the code ``Throttled``.
        raises: What every batch call raises, when set.
        lookup_raises: What the dead-letter queue lookup raises, when set.
    """

    def __init__(
        self,
        redrive: dict[str, dict[str, Any]] | None = None,
        *,
        refuse: frozenset[str] = frozenset(),
        raises: Exception | None = None,
        lookup_raises: Exception | None = None,
    ) -> None:
        self._redrive = redrive or {}
        self._refuse = refuse
        self._raises = raises
        self.lookup_raises = lookup_raises
        self.calls: Counter[str] = Counter()
        self.batches: list[tuple[str, str, list[dict[str, Any]]]] = []
        self.sent: list[dict[str, Any]] = []

    def send_message(self, **request: Any) -> dict[str, str]:
        self.calls["send_message"] += 1
        self.sent.append(request)
        return {"MessageId": "m-1"}

    def send_message_batch(self, QueueUrl: str, Entries: list[dict[str, Any]]) -> dict[str, Any]:  # noqa: N803
        return self._batch("send_message_batch", QueueUrl, Entries)

    def change_message_visibility_batch(
        self,
        QueueUrl: str,  # noqa: N803
        Entries: list[dict[str, Any]],  # noqa: N803
    ) -> dict[str, Any]:
        return self._batch("change_message_visibility_batch", QueueUrl, Entries)

    def delete_message(self, **request: Any) -> dict[str, Any]:
        self.calls["delete_message"] += 1
        return {}

    def delete_message_batch(self, QueueUrl: str, Entries: list[dict[str, Any]]) -> dict[str, Any]:  # noqa: N803
        return self._batch("delete_message_batch", QueueUrl, Entries)

    def get_queue_attributes(self, QueueUrl: str, AttributeNames: list[str]) -> dict[str, Any]:  # noqa: N803
        self.calls["get_queue_attributes"] += 1
        if self.lookup_raises is not None:
            raise self.lookup_raises
        policy = self._redrive.get(QueueUrl)
        return {} if policy is None else {"Attributes": {"RedrivePolicy": json.dumps(policy)}}

    def get_queue_url(self, QueueName: str, QueueOwnerAWSAccountId: str) -> dict[str, str]:  # noqa: N803
        self.calls["get_queue_url"] += 1
        return {
            "QueueUrl": f"https://sqs.eu-west-1.amazonaws.com/{QueueOwnerAWSAccountId}/{QueueName}"
        }

    def _batch(self, method: str, url: str, entries: list[dict[str, Any]]) -> dict[str, Any]:
        self.calls[method] += 1
        self.batches.append((method, url, entries))
        if self._raises is not None:
            raise self._raises
        failed = [
            {"Id": entry["Id"], "Code": "Throttled", "SenderFault": False}
            for entry in entries
            if entry["Id"] in self._refuse
        ]
        return {"Successful": [], "Failed": failed}

    def close(self) -> None:
        self.calls["close"] += 1

    def entries(self, method: str, url: str | None = None) -> list[dict[str, Any]]:
        """Every entry *method* carried, in order; of the queue at *url* when given."""
        return [
            entry
            for called, target, batch in self.batches
            if called == method and url in (None, target)
            for entry in batch
        ]

    @property
    def metadata_calls(self) -> int:
        """The calls that only describe a queue: they send and settle nothing."""
        return self.calls["get_queue_attributes"] + self.calls["get_queue_url"]
