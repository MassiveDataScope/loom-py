"""Per-document degradation accounting for Mongo value serialization."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field

from loom.core.logger.failure import public_form

_log = logging.getLogger(__name__)

_FAILURE_WARNINGS_LIMIT = 10


@dataclass(slots=True)
class SerializationStats:
    failures: int = 0
    last_error: str = ""
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record(self, exc: BaseException) -> int:
        with self._lock:
            self.failures += 1
            self.last_error = public_form(exc)
            return self.failures


_serialization_stats = SerializationStats()


def _record_serialization_failure(exc: BaseException, value: object) -> None:
    count = _serialization_stats.record(exc)
    log = _log.warning if count <= _FAILURE_WARNINGS_LIMIT else _log.debug
    log(
        "MongoSourceReader: could not serialise value of type %s (%s: %s) — replacing with null",
        type(value).__name__,
        type(exc).__name__,
        exc,
    )
