"""Per-document degradation accounting for Mongo value serialization."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field

from loom.core.errors.summary import ErrorSummary
from loom.etl.event import EtlEvent, EtlKey

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
            self.last_error = ErrorSummary.of(exc).describe()
            return self.failures


_serialization_stats = SerializationStats()


def _record_serialization_failure(exc: BaseException, value: object) -> None:
    count = _serialization_stats.record(exc)
    log = _log.warning if count <= _FAILURE_WARNINGS_LIMIT else _log.debug
    log(
        EtlEvent.SOURCE_VALUE_UNSERIALISABLE,
        extra={EtlKey.VALUE_TYPE: type(value).__name__, **ErrorSummary.of(exc).fields()},
    )
