"""Runtime profiles: the connection defaults each entry point contributes.

An entry point knows how many operations one of its processes runs at once —
a server many, a prefork worker one — and so it, not the configuration, owns
the default size of the connection pool. Each entry point hands its
:class:`RuntimeProfile` to the persistence it builds; a value declared in
``database.pool`` always wins over the profile, key by key.

This module imports no driver, so a host can name a profile without the
SQLAlchemy extra installed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class PoolDefaults:
    """Complete connection-pool settings: every value resolved, none optional.

    Args:
        size: Connections kept open, above zero.
        max_overflow: Extra connections allowed under load, zero or above.
        timeout_s: Seconds a caller waits for a free connection, above zero.
        recycle_s: Seconds after which a connection is replaced, above zero.
        pre_ping: Whether a connection is checked before it is handed out.

    Raises:
        ValueError: When a value has no valid meaning, naming the field.
    """

    size: int
    max_overflow: int
    timeout_s: int
    recycle_s: int
    pre_ping: bool

    def __post_init__(self) -> None:
        for name in ("size", "timeout_s", "recycle_s"):
            if getattr(self, name) <= 0:
                raise ValueError(f"PoolDefaults.{name} must be above zero")
        if self.max_overflow < 0:
            raise ValueError("PoolDefaults.max_overflow must be zero or above")


@dataclass(frozen=True)
class RuntimeProfile:
    """The defaults an entry point contributes to the persistence it builds.

    Args:
        name: Entry point label (``server``, ``worker``, ...); metrics carry it.
        pool: Connection-pool defaults for one process of this entry point.
    """

    name: str
    pool: PoolDefaults


class PoolMetricsSink(Protocol):
    """Receiver of connection-pool measurements; implemented by a metrics backend."""

    def register_engine(self, engine: Any, database: str) -> None:
        """Start reporting the gauges of *engine*'s current pool under the *database* label.

        The engine, not its pool, is handed over: disposing an engine replaces
        its pool, and the gauges must follow the replacement.
        """
        ...

    def observe_checkout(self, database: str, seconds: float) -> None:
        """Record how long one checkout from the *database* pool took."""
        ...

    def count_checkout_timeout(self, database: str) -> None:
        """Record one checkout from the *database* pool that ran out of time."""
        ...


@dataclass(frozen=True)
class BackendRuntime:
    """What the host hands a persistence backend besides its configuration.

    Args:
        profile: Defaults of the entry point building the backend.
        pool_metrics: Receiver of pool measurements; ``None`` when metrics are
            disabled, in which case the pool is built exactly as without them.
        connection_only: Serve the connection and nothing else, for a host
            with no model (a worker running raw-SQL jobs). A backend that
            honours it (``sqlalchemy``) then registers its session manager
            and unit of work, builds no cursor codec, registers no repository
            and leaves the compiled registry as it is. Its startup checks
            open one connection, run ``SELECT 1`` and release the pool: no
            table is created, no external schema or connecting role is
            inspected and no column is checked. Row-level security settings
            stay installed but unbound, so they fail closed.
    """

    profile: RuntimeProfile
    pool_metrics: PoolMetricsSink | None = None
    connection_only: bool = False


SERVER_PROFILE = RuntimeProfile(
    name="server",
    pool=PoolDefaults(size=5, max_overflow=5, timeout_s=10, recycle_s=1800, pre_ping=True),
)
"""A server process: many requests in flight on one event loop."""

WORKER_PROFILE = RuntimeProfile(
    name="worker",
    pool=PoolDefaults(size=2, max_overflow=0, timeout_s=30, recycle_s=1800, pre_ping=True),
)
"""A worker process running one task at a time (Celery prefork)."""

STREAMING_PROFILE = RuntimeProfile(name="streaming", pool=WORKER_PROFILE.pool)
"""A streaming worker writing batches; sized like a worker."""


__all__ = [
    "SERVER_PROFILE",
    "STREAMING_PROFILE",
    "WORKER_PROFILE",
    "BackendRuntime",
    "PoolDefaults",
    "PoolMetricsSink",
    "RuntimeProfile",
]
