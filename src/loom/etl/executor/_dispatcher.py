"""Parallel dispatcher protocol and its thread-pool implementation.

The dispatcher is the extension point for how parallel step/process groups
are executed.  The executor delegates all parallelism decisions here,
keeping itself free of scheduling concerns.  :class:`ThreadDispatcher` is
the default; any object implementing :class:`ParallelDispatcher` replaces
it::

    ETLExecutor(reader, writer, dispatcher=ThreadDispatcher(max_workers=4))
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

    * Run each task in a copy of the caller's context, so a step keeps the
      run's context variables (its flow run) and OpenTelemetry context (its
      trace parent).
    * Execute all tasks (not short-circuit on first failure).
    * Re-raise the first exception encountered after all tasks complete.

    Example::

        class PoolDispatcher:
            def run_all(self, tasks: Sequence[Callable[[], None]]) -> None:
                with ThreadPoolExecutor() as pool:
                    futures = [pool.submit(contextvars.copy_context().run, t) for t in tasks]
                for future in futures:
                    future.result()
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
    during Rust operations).  CPU-bound pure-Python steps hold the GIL, so
    their threads do not run at the same time.

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
