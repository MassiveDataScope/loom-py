"""Parallel dispatcher protocol and built-in implementations.

The dispatcher is the extension point for how parallel step/process groups
are executed.  The executor delegates all parallelism decisions here,
keeping itself free of scheduling concerns.

Swap implementations to move from local threads to distributed workers::

    # Local (default)
    ETLExecutor(reader, writer, dispatcher=ThreadDispatcher())

    # Celery (sprint 5+)
    ETLExecutor(reader, writer, dispatcher=CeleryGroupDispatcher(app))

    # Databricks Jobs (enterprise)
    ETLExecutor(reader, writer, dispatcher=DatabricksJobDispatcher(client))
"""

from __future__ import annotations

import contextvars
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Protocol, runtime_checkable


@runtime_checkable
class ParallelDispatcher(Protocol):
    """Protocol for executing a group of independent tasks in parallel.

    All tasks in a group are independent — the executor guarantees no
    data dependency between them.  Implementations must:

    * Execute all tasks (not short-circuit on first failure).
    * Re-raise the first exception encountered after all tasks complete.

    Example::

        class CeleryGroupDispatcher:
            def run_all(self, tasks: Sequence[Callable[[], None]]) -> None:
                group(celery_wrap(t) for t in tasks).apply_async().get()
    """

    def run_all(self, tasks: Sequence[Callable[[], None]]) -> None:
        """Execute all tasks in parallel and wait for completion.

        Args:
            tasks: Independent zero-argument callables to run concurrently.

        Raises:
            Exception: First exception raised by any task, after all tasks
                       have been given the opportunity to complete.
        """
        ...


class ThreadDispatcher:
    """Parallel dispatcher backed by :class:`~concurrent.futures.ThreadPoolExecutor`.

    Suitable for I/O-bound workloads and Polars steps (Polars releases the GIL
    during Rust operations).  For CPU-bound pure-Python steps prefer
    ``ProcessDispatcher`` or a Celery-based dispatcher.

    All tasks run to completion before any exception is re-raised — the
    observer receives error events for each failed task individually.

    Args:
        max_workers: Maximum thread pool size.  Defaults to the number of
                     tasks in the group (one thread per task).

    Example::

        dispatcher = ThreadDispatcher(max_workers=4)
        ETLExecutor(reader, writer, dispatcher=dispatcher)
    """

    def __init__(self, max_workers: int | None = None) -> None:
        self._max_workers = max_workers

    def run_all(self, tasks: Sequence[Callable[[], None]]) -> None:
        """Submit all tasks, wait for all to finish, re-raise first exception."""
        if not tasks:
            return
        workers = self._max_workers or len(tasks)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # Each task runs in a copy of the caller's context, as asyncio.to_thread does.
            futures = [pool.submit(contextvars.copy_context().run, task) for task in tasks]
            first_exc: Exception | None = None
            for future in futures:
                try:
                    future.result()
                except Exception as exc:
                    if first_exc is None:
                        first_exc = exc
        if first_exc is not None:
            raise first_exc
