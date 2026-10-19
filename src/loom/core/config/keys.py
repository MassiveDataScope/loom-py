"""Canonical configuration section keys used across bootstrap layers."""

from __future__ import annotations

from enum import StrEnum


class ConfigKey(StrEnum):
    """Top-level YAML section keys consumed by :func:`~loom.core.config.loader.section`.

    Using ``StrEnum`` keeps references typo-proof while remaining compatible
    with any API that expects plain ``str`` section identifiers.

    Example::

        cfg = section(raw, ConfigKey.CELERY, CeleryConfig)
    """

    APP = "app"
    AI = "ai"
    CACHE = "cache"
    DATABASE = "database"
    PERSISTENCE = "persistence"
    PERSISTENCE_CURSORS = "persistence.cursors"
    CELERY = "celery"
    KAFKA = "kafka"
    MONGO = "mongo"
    SQL = "sql"
    STORAGE = "storage"
    STREAMING = "streaming"
    STREAMING_RUNTIME = "streaming.runtime"
    STREAMING_SINKS = "streaming.sinks"
    OBSERVABILITY = "observability"
    JOBS = "jobs"
