"""The field keys of the Celery runtime."""

from __future__ import annotations

from loom.core.logger import LogKey


class CeleryLogField(LogKey):
    """The field keys only the Celery runtime writes."""

    COUNTDOWN = "loom.celery.countdown"
    PRIORITY = "loom.celery.priority"
    CALLBACK = "loom.celery.callback"
