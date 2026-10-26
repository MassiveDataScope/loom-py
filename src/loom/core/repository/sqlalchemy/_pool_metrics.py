"""Checkout timing for a connection pool, built only when pool metrics are enabled.

Without a :class:`~loom.core.persistence.profile.PoolMetricsSink` the engine
keeps its dialect's own pool class untouched, so disabled metrics cost nothing
per checkout.
"""

from __future__ import annotations

import time
from typing import Any

from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.pool import Pool

from loom.core.persistence.profile import PoolMetricsSink


def instrumented_pool_class(base: type[Pool], sink: PoolMetricsSink, database: str) -> type[Pool]:
    """Return a subclass of *base* reporting each checkout to *sink*.

    ``Pool.connect`` is the public entry every checkout goes through; the
    measured duration includes the pre-ping when it is enabled.
    """

    class _InstrumentedPool(base):  # type: ignore[valid-type,misc]
        def connect(self) -> Any:
            started = time.perf_counter()
            try:
                connection = super().connect()
            except PoolTimeoutError:
                sink.count_checkout_timeout(database)
                raise
            sink.observe_checkout(database, time.perf_counter() - started)
            return connection

    _InstrumentedPool.__name__ = f"Instrumented{base.__name__}"
    _InstrumentedPool.__qualname__ = _InstrumentedPool.__name__
    return _InstrumentedPool


__all__ = ["instrumented_pool_class"]
