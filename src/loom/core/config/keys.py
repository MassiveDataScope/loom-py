"""Canonical configuration section keys used across bootstrap layers."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

INTERPOLATION: Final = "${"
"""What opens a placeholder (an interpolation) in a value as written."""


class ConfigKey(StrEnum):
    """Top-level YAML section keys consumed by :func:`~loom.core.config.loader.section`.

    Using ``StrEnum`` keeps references typo-proof while remaining compatible
    with any API that expects plain ``str`` section identifiers.

    Example::

        cfg = section(raw, ConfigKey.CELERY, CeleryConfig)
    """

    APP = "app"
    AI = "ai"
    AWS = "aws"
    CACHE = "cache"
    PERSISTENCE = "persistence"
    PERSISTENCE_CURSORS = "persistence.cursors"
    CELERY = "celery"
    KAFKA = "kafka"
    MONGO = "mongo"
    SQL = "sql"
    SQS = "sqs"
    STORAGE = "storage"
    STORES = "stores"
    STREAMING = "streaming"
    STREAMING_RUNTIME = "streaming.runtime"
    STREAMING_SINKS = "streaming.sinks"
    OBSERVABILITY = "observability"
    JOBS = "jobs"
    LAMBDA = "lambda"
