"""The configuration a Lambda ``jobs`` function needs: the queue it consumes."""

from __future__ import annotations

from typing import Any

JOBS_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/jobs"


def jobs_trigger(**extra: Any) -> dict[str, Any]:
    """A ``jobs`` trigger consuming the queue ``jobs``."""
    return {"source": "jobs", "queue": {"name": "jobs"}, **extra}


def jobs_queue() -> dict[str, Any]:
    """The ``aws`` and ``sqs`` sections mapping the queue ``jobs``, which discards."""
    return {
        "aws": {"region": "eu-west-1"},
        "sqs": {"queues": {"jobs": {"url": JOBS_URL, "abandoned": "discard"}}},
    }
