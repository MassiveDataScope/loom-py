"""The Celery runtime: how a worker composes and serves an application.

The worker runs the same recipe as every other entry point
(:func:`loom.composition.create_app`), so it gains what the API already had:
any store backend, row-level security bound in
``prepare_models``, the cache gateways closed on the way out, and
``JobService`` and ``SqlQueryService`` in its container.  What stays specific
to the worker is its discovery, its pool profile, and when the Celery
signals check, open and close the application.  Its recipe declares
``agents=False``, so the composition root refuses any ``Agent()`` or
``Mcp()`` marker at start-up:

* prefork: ``worker_init`` checks once in the parent and opens nothing;
  ``worker_process_init`` opens each child with ``check=False``;
  ``worker_process_shutdown`` and ``worker_shutdown`` close, best effort.
* solo, threads and every other pool: ``worker_init`` checks and opens;
  ``worker_shutdown`` closes.

A worker with no model asks for its connection only
(``StorePluginContext.connection_only``), serving no repository: its check
probes the connection and creates no table, though on Postgres it still
refuses a superuser or bypass role and scoped tables outside an external
schema; its row-level security is never bound, so on Postgres with
``schema.mode: external`` every session it opens fails closed instead of
running unscoped.

Fork safety follows from :meth:`~loom.composition.Application.check`: it
leaves no connection open, so nothing the parent touched crosses the fork.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Protocol, cast

from billiard import get_all_start_methods, set_start_method  # type: ignore[import-untyped]
from celery import Celery  # type: ignore[import-untyped]
from celery.concurrency import get_implementation  # type: ignore[import-untyped]
from celery.concurrency.prefork import TaskPool as PreforkPool  # type: ignore[import-untyped]
from celery.exceptions import WorkerTerminate  # type: ignore[import-untyped]
from celery.signals import (  # type: ignore[import-untyped]
    worker_init,
    worker_process_init,
    worker_process_shutdown,
    worker_shutdown,
)

from loom.celery._discovery import (
    _emit_worker_init_graph,
    _resolve_compilables_and_jobs,
)
from loom.celery.config import CeleryConfig, CeleryRuntimeConfig, create_celery_app
from loom.celery.runner import (
    REDELIVERY,
    _CeleryProcessBridge,
    _make_callback_task,
    _make_job_task,
)
from loom.composition import (
    Application,
    ApplicationHost,
    CompositionErrorCode,
    CompositionStateError,
    Project,
    Recipe,
)
from loom.core.async_bridge import build_backend_options
from loom.core.checks import CheckScope
from loom.core.config import ConfigError, ConfigKey
from loom.core.discovery.worker import (
    WorkerComponents,
)
from loom.core.engine.compilable import Compilable
from loom.core.job.callback import CallbackOutcome
from loom.core.logger import LogRuntime
from loom.core.runner import shutdown_runner
from loom.core.stores import WORKER_PROFILE

if TYPE_CHECKING:
    from loom.core.engine.execution_observer import ExecutionObserver
    from loom.core.job.job import Job

_log = logging.getLogger(__name__)

_EX_FAILURE = 1
"""Exit status of a worker that cannot start."""

_SIGNAL_UIDS = {
    worker_init: "loom.celery.worker.init",
    worker_process_init: "loom.celery.worker.process_init",
    worker_process_shutdown: "loom.celery.worker.process_shutdown",
    worker_shutdown: "loom.celery.worker.shutdown",
}


class _ProcessBridgeProtocol(Protocol):
    backend: str

    def initialize(self) -> None: ...

    def refuse(self, cause: Exception) -> None: ...

    def run(self, coro: Any, *, eager_fallback: bool) -> Any: ...

    def shutdown(self) -> None: ...


def _build_process_bridge(runtime_cfg: CeleryRuntimeConfig) -> _CeleryProcessBridge:
    """Build the per-process bridge used by Celery worker tasks."""
    return _CeleryProcessBridge(
        backend=runtime_cfg.backend,
        backend_options=build_backend_options(runtime_cfg.backend, runtime_cfg.use_uvloop),
        shutdown_timeout_ms=runtime_cfg.shutdown_timeout_ms,
    )


def _is_prefork(worker: Any) -> bool:
    """Whether *worker* (the ``worker_init`` sender) forks its pool.

    At ``worker_init`` the controller still holds the configured alias
    (``prefork``, ``processes``) or class; Celery resolves it right after.
    """
    pool = get_implementation(getattr(worker, "pool_cls", None) or "prefork")
    return isinstance(pool, type) and issubclass(pool, PreforkPool)


_Step = Callable[[], Coroutine[Any, Any, Any]]
"""One bridge call that opens or closes the application."""


class _WorkerLifecycle:
    """Checks, opens and closes one worker process's application.

    The application is opened on the async bridge that runs the tasks, so a
    connection is always created on the loop that uses it. An
    :class:`~loom.composition.ApplicationHost` opens and closes it in one task
    of the bridge, although each bridge call runs in a task of its own: a
    task-affine resource (an agent host, an MCP client) closes cleanly.
    Another backend than asyncio has no asyncio task to own it: there the
    application is entered and exited in two bridge calls, which is safe
    because the worker hosts no agents. Closing is idempotent and best
    effort: neither shutdown signal is guaranteed, and a failure to close is
    logged, never raised into Celery.

    Args:
        application: The composed application of the worker.
        process_bridge: Per-process async bridge the tasks run on.
    """

    def __init__(self, application: Application, process_bridge: _ProcessBridgeProtocol) -> None:
        self._application = application
        self._runtime = process_bridge
        self._stop: _Step | None = None

    def on_worker_init(self, worker: Any) -> None:
        """Check once; open here too unless the pool forks children that open.

        Raises:
            WorkerTerminate: When the checks or the opening fail, so the worker
                does not start (Celery logs and swallows a plain exception
                raised by a signal handler).
        """
        try:
            if _is_prefork(worker):
                self._fork_children(worker)
                asyncio.run(self._application.check())
            else:
                self._open(check=True)
        except Exception as exc:
            _log.exception("worker application failed to start")
            raise WorkerTerminate(_EX_FAILURE) from exc

    @staticmethod
    def _fork_children(worker: Any) -> None:
        """Pin ``fork`` as the pool start method, or refuse ``spawn`` and ``forkserver``.

        Raises:
            ConfigError: When *worker* starts its pool with ``spawn`` or ``forkserver``.
        """
        method = worker.pool_start_method
        if method in {"spawn", "forkserver"}:
            raise ConfigError(
                "loom registers tasks in the parent and needs fork "
                "(worker_pool_start_method: fork); a prefork pool started with "
                f"{method} does not inherit them"
            )
        if method == "fork" and "fork" in get_all_start_methods():
            set_start_method("fork", force=True)

    def on_process_init(self) -> None:
        """Open in a prefork child; the parent already checked.

        A failure is logged once and recorded on the bridge, so every task of
        this child fails with :class:`~loom.celery.errors.WorkerNotOpenError`
        chained to it. The child is not killed: the pool would respawn it into
        the same failure.
        """
        try:
            self._open(check=False)
        except Exception as exc:
            _log.exception("worker process failed to open its application")
            self._runtime.refuse(exc)

    def close(self) -> None:
        """Close what this process opened, then stop its bridge."""
        stop, self._stop = self._stop, None
        stop_coroutine: Any = None
        try:
            if stop is not None:
                stop_coroutine = stop()
                self._runtime.run(stop_coroutine, eager_fallback=False)
        except Exception:
            _log.warning("worker application failed to close", exc_info=True)
            if stop_coroutine is not None:
                stop_coroutine.close()
        finally:
            shutdown_runner(self._runtime)

    def _open(self, *, check: bool) -> None:
        self._runtime.initialize()
        start, stop = self._opening(check=check)
        try:
            self._runtime.run(start(), eager_fallback=False)
        except BaseException:
            shutdown_runner(self._runtime)
            raise
        self._stop = stop

    def _opening(self, *, check: bool) -> tuple[_Step, _Step]:
        """How this process opens and closes its application, each a bridge call."""
        if self._runtime.backend == "asyncio":
            host = ApplicationHost(self._application, check=check)
            return host.start, host.shutdown
        running = self._application.running(check=check)
        stack = AsyncExitStack()
        return partial(stack.enter_async_context, running), stack.aclose


def _connect_lifecycle_signals(lifecycle: _WorkerLifecycle) -> None:
    """Connect the Celery worker signals to *lifecycle*.

    Each handler carries a ``dispatch_uid``, so composing a worker again (in
    integration tests, say) replaces the previous handlers instead of
    accumulating them: only the latest application is checked, opened and
    closed.
    """

    def _on_worker_init(sender: Any = None, **kwargs: Any) -> None:
        lifecycle.on_worker_init(sender)

    def _on_process_init(**kwargs: Any) -> None:
        lifecycle.on_process_init()

    def _on_shutdown(**kwargs: Any) -> None:
        lifecycle.close()

    handlers = {
        worker_init: _on_worker_init,
        worker_process_init: _on_process_init,
        worker_process_shutdown: _on_shutdown,
        worker_shutdown: _on_shutdown,
    }
    for signal, handler in handlers.items():
        uid = _SIGNAL_UIDS[signal]
        signal.disconnect(dispatch_uid=uid)
        signal.connect(handler, weak=False, dispatch_uid=uid)


def _register_tasks(
    celery_app: Celery,
    declared: _Declared,
    application: Application,
    process_bridge: _CeleryProcessBridge,
) -> None:
    """Register one task per job and one per callback class and outcome."""
    kernel, observability = application.kernel, application.observability
    if declared.jobs:
        _log.info(
            "Job retry policies",
            extra={
                "transport": "celery",
                "jobs": application.job_settings.describe_retries(REDELIVERY, declared.jobs),
            },
        )
    for job_type in declared.jobs:
        _make_job_task(
            celery_app,
            job_type,
            kernel.factory,
            kernel.executor,
            process_bridge,
            kernel.execution_observer,
            observability=observability,
            settings=application.job_settings,
        )
    for callback_type in application.discovered.callbacks:
        for outcome in CallbackOutcome:
            _make_callback_task(
                celery_app,
                callback_type,
                outcome,
                kernel.factory,
                process_bridge,
                observability=observability,
            )


@dataclass(frozen=True)
class _Declared:
    """What :meth:`CeleryRuntime.recipe` resolved and :meth:`CeleryRuntime.mount` registers."""

    celery: CeleryConfig
    jobs: tuple[type[Job[Any]], ...]


class CeleryRuntime:
    """Serve a composed application as a Celery worker.

    One instance composes one application: :meth:`recipe` keeps the jobs,
    the discovered job callbacks and the Celery configuration it resolves for
    :meth:`mount`.

    Args:
        jobs: ``Job`` subclasses to register as tasks; when empty, they are
            discovered from ``app.packages``.
        use_cases: Use cases to compile alongside the jobs, so a callback or
            a job can invoke them.
        interfaces: ``RestInterface`` subclasses whose route use cases are
            compiled in the worker.
        execution_observer: Observer of use-case and job events, or ``None``.
    """

    def __init__(
        self,
        *,
        jobs: Sequence[type[Job[Any]]] = (),
        use_cases: Sequence[type[Compilable]] = (),
        interfaces: Sequence[type[Any]] = (),
        execution_observer: ExecutionObserver | None = None,
    ) -> None:
        self._jobs = tuple(jobs)
        self._use_cases = tuple(use_cases)
        self._interfaces = tuple(interfaces)
        self._execution_observer = execution_observer
        self._declared: _Declared | None = None

    def recipe(self, project: Project) -> Recipe:
        """Decode ``celery`` and discover the worker's components; the composition applies ``jobs``.

        Raises:
            ConfigError: When ``celery`` or ``stores`` is misdeclared, or no
                backend is registered under the name the worker resolves.
            RuntimeError: When no job is passed and no ``app.packages`` is
                configured.
            CompositionStateError: When this instance already composed an
                application.
        """
        if self._declared is not None:
            raise CompositionStateError(
                CompositionErrorCode.RUNTIME_ALREADY_COMPOSED.issue(component="CeleryRuntime")
            )
        ctx = project.config
        celery_cfg = ctx.section(ConfigKey.CELERY, CeleryConfig)
        resolved = _resolve_compilables_and_jobs(project.discovered, self._jobs, self._use_cases)
        compilables = tuple(
            dict.fromkeys(
                (
                    *resolved.compilables,
                    *(
                        route.use_case
                        for interface in self._interfaces
                        for route in interface.routes
                    ),
                )
            )
        )
        resolved = WorkerComponents(
            compilables=compilables,
            jobs=resolved.jobs,
            models=tuple(dict.fromkeys(resolved.models)),
        )
        _emit_worker_init_graph(resolved)

        self._declared = _Declared(celery=celery_cfg, jobs=resolved.jobs)
        return Recipe(
            profile=WORKER_PROFILE,
            discovered=project.discovered.with_changes(
                models=resolved.models,
                use_cases=cast(Any, resolved.compilables),
                jobs=resolved.jobs,
            ),
            check_scope=CheckScope.without_rest(),
            agents=False,
            log_runtime=LogRuntime.CELERY,
            execution_observer=self._execution_observer,
            connection_only=not resolved.models,
        )

    def mount(self, application: Application) -> Celery:
        """Register the tasks and connect the lifecycle signals.

        Raises:
            CompositionStateError: When :meth:`recipe` has not run on this instance.
        """
        if self._declared is None:
            raise CompositionStateError(
                CompositionErrorCode.RUNTIME_RECIPE_MISSING.issue(component="CeleryRuntime")
            )
        declared = self._declared

        process_bridge = _build_process_bridge(declared.celery.runtime)
        celery_app = create_celery_app(declared.celery)
        _register_tasks(celery_app, declared, application, process_bridge)
        _connect_lifecycle_signals(_WorkerLifecycle(application, process_bridge))
        return celery_app


__all__ = ["CeleryRuntime"]
