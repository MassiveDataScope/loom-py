"""Canonical configuration section keys used across bootstrap layers."""

from __future__ import annotations

from enum import StrEnum

INTERPOLATION = "${"


class ConfigKey(StrEnum):
    """Top-level YAML section keys consumed by :func:`~loom.core.config.loader.section`.

    Using ``StrEnum`` keeps references typo-proof while remaining compatible
    with any API that expects plain ``str`` section identifiers.

    Example::

        cfg = section(raw, ConfigKey.CELERY, CeleryConfig)
    """

    APP = "app"
    APP_REST = "app.rest"
    AI = "ai"
    AWS = "aws"
    CACHE = "cache"
    CELERY = "celery"
    KAFKA = "kafka"
    MONGO = "mongo"
    SQL = "sql"
    SQS = "sqs"
    STORAGE = "storage"
    STORES = "stores"
    STORES_SETTINGS = "stores.settings"
    STREAMING = "streaming"
    STREAMING_RUNTIME = "streaming.runtime"
    STREAMING_SINKS = "streaming.sinks"
    OBSERVABILITY = "observability"
    JOBS = "jobs"
    LAMBDA = "lambda"
