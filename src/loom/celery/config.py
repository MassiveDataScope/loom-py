"""Celery and Job configuration types.

``CeleryConfig`` holds broker/worker settings extracted from the OmegaConf
object passed to ``bootstrap_worker()``.  ``JobConfig`` carries per-job
routing overrides that supersede the ClassVar defaults declared on each
:class:`~loom.core.job.job.Job` subclass.

Both types use ``msgspec.Struct`` for consistency with the rest of the
framework — they are validated at bootstrap time through the config section.

Typical YAML layout::

    celery:
      broker_url: "redis://redis:6379/0"
      result_backend: "redis://redis:6379/1"
      worker_concurrency: 8
      worker_prefetch_multiplier: 1
      runtime:
        backend: "asyncio"
        use_uvloop: true

    jobs:
      RecalcPricesJob:
        queue: "prices.heavy"
        retries: 5
        timeout: 600
      SendWelcomeEmailJob:
        retries: 3
"""

from __future__ import annotations

from typing import Literal

import msgspec
from celery import Celery  # type: ignore[import-untyped]
from kombu import Queue  # type: ignore[import-untyped]

from loom.core.job.config import JobConfig as JobConfig
from loom.core.job.config import apply_job_config as apply_job_config
from loom.core.model import LoomFrozenStruct
from loom.core.model.redaction import Dsn
from loom.core.model.types import PositiveInt


class CeleryRuntimeConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Async bridge settings for a Celery worker process.

    Attributes:
        backend: AnyIO backend used by :class:`~loom.core.async_bridge.AsyncBridge`.
            Accepted values are ``"asyncio"`` and ``"trio"``.
        use_uvloop: When ``True`` and the backend is ``"asyncio"``, the worker
            uses uvloop on supported platforms.
        shutdown_timeout_ms: Maximum milliseconds to wait for in-flight async
            work during worker shutdown, above zero.  ``None`` waits
            indefinitely.
    """

    backend: Literal["asyncio", "trio"] = "asyncio"
    use_uvloop: bool = False
    shutdown_timeout_ms: PositiveInt | None = None


class CeleryConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Broker and worker settings for the Celery application.

    Attributes:
        broker_url: URL for the message broker (e.g. ``"redis://redis:6379/0"``).
        result_backend: URL for the result backend.
        worker_concurrency: Number of concurrent worker processes.
        worker_max_tasks_per_child: Maximum tasks a worker executes before
            being replaced.  Helps prevent memory leaks.
        worker_prefetch_multiplier: Tasks prefetched per worker.  Set to
            ``1`` for fair dispatch with heterogeneous task durations.
        task_always_eager: When ``True`` tasks execute synchronously in the
            calling process.  Set to ``True`` in integration tests.
        timezone: Celery timezone string.
        enable_utc: Whether Celery stores datetimes in UTC.
        task_serializer: Serialization format for task arguments.
            Defaults to ``"json"``.
        result_serializer: Serialization format for task results.
            Defaults to ``"json"``.
        accept_content: List of accepted content types.  Defaults to
            ``["json"]``.
        queues: Explicit queue names to declare.  Celery creates them on
            demand when empty.
        task_default_queue: Default queue for tasks without an explicit
            ``queue=`` argument — including callback signatures produced by
            ``link`` / ``link_error``.  Must be one of ``queues`` when that
            list is non-empty.  Defaults to ``"default"``.
        runtime: AnyIO bridge settings for worker-side async execution.

    Example::

        cfg = section(omegaconf_obj, "celery", CeleryConfig)
    """

    broker_url: Dsn
    result_backend: Dsn
    worker_concurrency: int = 4
    worker_max_tasks_per_child: int = 1_000
    worker_prefetch_multiplier: int = 1
    task_always_eager: bool = False
    timezone: str = "UTC"
    enable_utc: bool = True
    task_serializer: str = "json"
    result_serializer: str = "json"
    accept_content: list[str] = msgspec.field(default_factory=lambda: ["json"])
    queues: list[str] = msgspec.field(default_factory=list)
    task_default_queue: str = "default"
    runtime: CeleryRuntimeConfig = msgspec.field(default_factory=CeleryRuntimeConfig)


def create_celery_app(cfg: CeleryConfig) -> Celery:
    """Build a Celery application from a ``CeleryConfig``.

    When ``cfg.queues`` is non-empty, the queues are pre-declared on the
    application as :class:`kombu.Queue` objects.  If the list is empty,
    Celery creates queues on demand when tasks are first dispatched.

    ``cfg.task_default_queue`` is validated against ``cfg.queues`` when the
    latter is non-empty: routing callbacks to an undeclared queue would leave
    them unconsumed silently.

    Args:
        cfg: Validated Celery configuration.

    Returns:
        Configured :class:`celery.Celery` application instance.

    Raises:
        ValueError: When ``task_default_queue`` is not listed in ``queues``
            and ``queues`` is non-empty.

    Example::

        celery_app = create_celery_app(cfg)
        celery_app.start()
    """
    if cfg.queues and cfg.task_default_queue not in cfg.queues:
        raise ValueError(
            f"task_default_queue={cfg.task_default_queue!r} is not declared in "
            f"queues={cfg.queues}. Callbacks would be silently routed to an "
            "unconsumed queue. Add the queue name to the 'queues' list."
        )
    app = Celery(set_as_current=False)
    app.conf.update(
        broker_url=cfg.broker_url.reveal(),
        result_backend=cfg.result_backend.reveal(),
        worker_concurrency=cfg.worker_concurrency,
        worker_max_tasks_per_child=cfg.worker_max_tasks_per_child,
        worker_prefetch_multiplier=cfg.worker_prefetch_multiplier,
        task_always_eager=cfg.task_always_eager,
        timezone=cfg.timezone,
        enable_utc=cfg.enable_utc,
        task_serializer=cfg.task_serializer,
        result_serializer=cfg.result_serializer,
        accept_content=cfg.accept_content,
        task_default_queue=cfg.task_default_queue,
    )
    if cfg.queues:
        app.conf.task_queues = [Queue(name) for name in cfg.queues]
    return app
