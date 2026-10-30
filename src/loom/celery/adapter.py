"""The Celery adapter: how a worker composes and serves an application.

The worker runs the same recipe as every other entry point
(:func:`loom.composition.create_app`), so it gains what the API already had:
any persistence backend, row-level security bound in
``prepare_models``, the cache gateways closed on the way out, and
``JobService`` and ``SqlQueryService`` in its container.  What stays specific
to the worker is its discovery, its pool profile, the agent markers it
refuses, and when the Celery signals check, open and close the application:

* prefork: ``worker_init`` checks once in the parent and opens nothing;
  ``worker_process_init`` opens each child with ``check=False``;
  ``worker_process_shutdown`` and ``worker_shutdown`` close, best effort.
* solo, threads and every other pool: ``worker_init`` checks and opens;
  ``worker_shutdown`` closes.

A worker with no model asks for its connection only
(``BackendRuntime.connection_only``), serving no repository: its check
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
from collections.abc import Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, cast

import msgspec
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
    _apply_job_config_if_present,
    _emit_worker_init_graph,
    _merge_unique,
    _resolve_compilables_and_jobs,
    _use_cases_from_interfaces,
    _WorkerResolved,
)
from loom.celery.config import CeleryConfig, CeleryRuntimeConfig, create_celery_app
from loom.celery.runner import (
    _CeleryAsyncRuntime,
    _make_callback_error_task,
    _make_callback_task,
    _make_job_task,
)
from loom.composition import Application, ApplicationHost, CompositionSource, Recipe
from loom.core.async_bridge import build_backend_options
from loom.core.bootstrap import KernelRuntime
from loom.core.checks import REST_CHECKS
from loom.core.config import ConfigContext, ConfigKey
from loom.core.config.errors import ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.engine.compilable import Compilable
from loom.core.persistence import (
    WORKER_PROFILE,
    resolve_backend,
)
from loom.core.persistence.database import DatabaseConfig
from loom.core.runner import shutdown_runner
from loom.core.use_case.agent_markers import declaring_agent_bindings
from loom.core.use_case.mcp_markers import declaring_mcp_bindings

if TYPE_CHECKING:
    from loom.core.engine.compiler import UseCaseCompiler
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job

_logger = logging.getLogger(__name__)

_EX_FAILURE = 1
"""Exit status of a worker that cannot start."""

_DATABASE_FIELDS = frozenset(field.encode_name for field in msgspec.structs.fields(DatabaseConfig))

_SIGNAL_UIDS = {
    worker_init: "loom.celery.worker.init",
    worker_process_init: "loom.celery.worker.process_init",
    worker_process_shutdown: "loom.celery.worker.process_shutdown",
    worker_shutdown: "loom.celery.worker.shutdown",
}


# ---------------------------------------------------------------------------
# Persistence backend of a worker
# ---------------------------------------------------------------------------


def _worker_backend_name(source: CompositionSource) -> str:
    """``persistence.backend``; else ``sqlalchemy`` for a ``database`` of its own, else ``none``.

    A worker that runs only pure jobs declares no database and pays for no
    persistence at all.

    Raises:
        ConfigError: When ``database`` is neither the worker's own section nor
            named databases (see :func:`_declares_own_database`).
    """
    # An empty default tells an absent key from a declared one.
    explicit = source.backend_name(default="")
    if explicit:
        return explicit
    return "sqlalchemy" if _declares_own_database(source.config) else "none"


def _declares_own_database(config: ConfigContext) -> bool:
    """Whether ``database`` is the worker's own section rather than absent or named entries.

    A section holding any key of the ``database`` contract is the worker's own
    and the backend decodes it strictly, so a missing or misspelt ``url``
    fails the worker. A section of named entries only (``database.<name>``)
    belongs to streaming sinks.

    Raises:
        ConfigError: When the section is neither: a key outside the contract
            whose value is not a named database mapping (``URL: ...``).
    """
    section = config.raw(ConfigKey.DATABASE)
    if not isinstance(section, Mapping):
        return False
    if _DATABASE_FIELDS.intersection(section):
        return True
    stray = sorted(str(key) for key, value in section.items() if not isinstance(value, Mapping))
    if stray:
        raise ConfigError(
            f"database.{stray[0]} is neither a key of the database section "
            f"({', '.join(sorted(_DATABASE_FIELDS))}) nor a named database"
        )
    return False


# ---------------------------------------------------------------------------
# Agent() and Mcp() marker refusal (T402, spec 015 T202)
# ---------------------------------------------------------------------------


def _reject_markers(
    compilables: Sequence[type[Compilable]],
    kernel: KernelRuntime,
    *,
    declaring_bindings: Callable[[Sequence[type[Compilable]], UseCaseCompiler], Sequence[object]],
    verifier: str,
) -> None:
    """Abort worker start-up when a compiled use case declares a marker of the AI pillar.

    A task worker never builds an AI runtime (its recipe declares
    ``agents=False``), so a use case declaring the marker would compile here
    and only fail once a task actually invoked it. Refusing at start-up
    instead reuses the check the API runs, ``loom.ai._startup.<verifier>``,
    called with nothing configured, which reports every declared name as
    unknown.

    Args:
        compilables: Every use case and job compiled for this worker.
        kernel: The kernel runtime, whose compiler already holds each of
            their plans and whose registry names them in the error.
        declaring_bindings: Returns the use cases declaring the marker.
        verifier: Name of the check in :mod:`loom.ai._startup`.

    Raises:
        AgentCompilationError: Naming each declaring use case and its marked
            parameter, when at least one declares the marker.
    """
    declaring = declaring_bindings(compilables, kernel.compiler)
    if not declaring:
        # Nothing declares the marker: importing 'loom.ai' here, only to find
        # nothing to check, would pull the AI pillar into every worker
        # process regardless of whether any job or use case ever reaches it.
        return

    # Local import: the AI pillar is optional and this branch only runs once
    # a declaring use case is already known to exist.
    from loom.ai import _startup

    verify: Callable[..., None] = getattr(_startup, verifier)
    verify(declaring, kernel.registry, {})


def _reject_agent_markers(compilables: Sequence[type[Compilable]], kernel: KernelRuntime) -> None:
    """Refuse an ``Agent()`` marker through :func:`loom.ai._startup.verify_agent_markers`."""
    _reject_markers(
        compilables,
        kernel,
        declaring_bindings=declaring_agent_bindings,
        verifier="verify_agent_markers",
    )


def _reject_mcp_markers(compilables: Sequence[type[Compilable]], kernel: KernelRuntime) -> None:
    """Refuse an ``Mcp()`` marker through :func:`loom.ai._startup.verify_mcp_markers`."""
    _reject_markers(
        compilables,
        kernel,
        declaring_bindings=declaring_mcp_bindings,
        verifier="verify_mcp_markers",
    )


# ---------------------------------------------------------------------------
# Celery worker lifecycle signals
# ---------------------------------------------------------------------------


class _AsyncRuntimeProtocol(Protocol):
    backend: str

    def initialize(self) -> None: ...

    def refuse(self, cause: BaseException) -> None: ...

    def run(self, coro: Any, *, eager_fallback: bool) -> Any: ...

    def shutdown(self) -> None: ...


def _build_async_runtime(runtime_cfg: CeleryRuntimeConfig) -> _CeleryAsyncRuntime:
    """Build the per-process async runtime used by Celery worker tasks."""
    return _CeleryAsyncRuntime(
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
    task-affine resource (an agent runtime, an MCP client) closes cleanly.
    Another backend than asyncio has no asyncio task to own it: there the
    application is entered and exited in two bridge calls, which is safe
    because the worker hosts no agents. Closing is idempotent and best
    effort: neither shutdown signal is guaranteed, and a failure to close is
    logged, never raised into Celery.

    Args:
        application: The composed application of the worker.
        async_runtime: Per-process async bridge the tasks run on.
    """

    def __init__(self, application: Application, async_runtime: _AsyncRuntimeProtocol) -> None:
        self._application = application
        self._runtime = async_runtime
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
                # No bridge in the parent: a thread must not cross the fork.
                asyncio.run(self._application.check())
            else:
                self._open(check=True)
        except Exception as exc:
            _logger.exception("worker application failed to start")
            raise WorkerTerminate(_EX_FAILURE) from exc

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
            _logger.exception("worker process failed to open its application")
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
            _logger.warning("worker application failed to close", exc_info=True)
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
            return host.start, host.stop
        running = self._application.running(check=check)
        return running.__aenter__, lambda: running.__aexit__(None, None, None)


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


# ---------------------------------------------------------------------------
# Task registration
# ---------------------------------------------------------------------------


def _register_tasks(
    celery_app: Celery,
    declared: _Declared,
    application: Application,
    async_runtime: _CeleryAsyncRuntime,
    metrics: MetricsAdapter | None,
) -> None:
    """Register one task per job and two per callback class."""
    kernel, observability = application.kernel, application.observability
    for job_type in declared.jobs:
        _make_job_task(
            celery_app,
            job_type,
            kernel.factory,
            kernel.executor,
            async_runtime,
            metrics,
            observability_runtime=observability,
        )
    for callback_type in declared.callbacks:
        _make_callback_task(
            celery_app,
            callback_type,
            kernel.factory,
            async_runtime,
            observability_runtime=observability,
        )
        _make_callback_error_task(
            celery_app,
            callback_type,
            kernel.factory,
            async_runtime,
            observability_runtime=observability,
        )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Declared:
    """What :meth:`CeleryAdapter.recipe` resolved and :meth:`CeleryAdapter.bind` registers."""

    celery: CeleryConfig
    jobs: tuple[type[Job[Any]], ...]
    callbacks: tuple[type[Any], ...]


class CeleryAdapter:
    """Serve a composed application as a Celery worker.

    One instance composes one application: :meth:`recipe` keeps the jobs,
    callbacks and Celery configuration it resolves for :meth:`bind`.

    Args:
        jobs: ``Job`` subclasses to register as tasks; when empty, they are
            discovered from ``app.discovery`` (``modules`` or ``manifest``).
        use_cases: Use cases to compile alongside the jobs, so a callback or
            a job can invoke them.
        interfaces: ``RestInterface`` subclasses whose route use cases are
            compiled in the worker.
        callbacks: ``JobCallback`` classes to register as success and
            failure tasks.
        metrics: Use-case and job metrics, or ``None``.
    """

    def __init__(
        self,
        *,
        jobs: Sequence[type[Job[Any]]] = (),
        use_cases: Sequence[type[Compilable]] = (),
        interfaces: Sequence[type[Any]] = (),
        callbacks: Sequence[type[Any]] = (),
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self._jobs = tuple(jobs)
        self._use_cases = tuple(use_cases)
        self._interfaces = tuple(interfaces)
        self._callbacks = tuple(callbacks)
        self._metrics = metrics
        self._declared: _Declared | None = None

    def recipe(self, source: CompositionSource) -> Recipe:
        """Decode ``celery``, discover the worker's components and apply the per-job config.

        Raises:
            ConfigError: When ``celery`` or ``database`` is misdeclared, or no
                backend is registered under the name the worker resolves.
            RuntimeError: When no job is passed and no ``app.discovery`` is
                configured, or this instance already composed an application.
        """
        if self._declared is not None:
            raise RuntimeError(
                "one adapter instance composes one application; create another adapter"
            )
        ctx = source.config
        celery_cfg = ctx.section(ConfigKey.CELERY, CeleryConfig)
        resolved = _resolve_compilables_and_jobs(ctx, self._jobs, self._use_cases)
        compilables = _merge_unique(
            resolved.compilables, _use_cases_from_interfaces(self._interfaces)
        )
        for job_type in resolved.jobs:
            _apply_job_config_if_present(ctx, job_type)
        resolved = _WorkerResolved(
            compilables=compilables,
            jobs=resolved.jobs,
            models=tuple(dict.fromkeys(resolved.models)),
            callbacks=_merge_unique(resolved.callbacks, self._callbacks),
        )
        _emit_worker_init_graph(resolved)
        backend = resolve_backend(_worker_backend_name(source))

        self._declared = _Declared(
            celery=celery_cfg, jobs=resolved.jobs, callbacks=resolved.callbacks
        )
        return Recipe(
            profile=WORKER_PROFILE,
            discovered=DiscoveryResult(
                models=resolved.models,
                # Jobs compile and register like any use case.
                use_cases=cast(Any, resolved.compilables),
                interfaces=(),
            ),
            backend=backend,
            skip_checks=frozenset({REST_CHECKS}),
            agents=False,
            metrics=self._metrics,
            # Without models the worker serves only a unit of work, as it always
            # has: no cursor keys needed, and models the caller compiled stay.
            connection_only=not resolved.models,
        )

    def bind(self, application: Application) -> Celery:
        """Refuse agent markers, register the tasks and connect the lifecycle signals.

        Raises:
            RuntimeError: When :meth:`recipe` has not run on this instance.
            AgentCompilationError: When a compiled use case declares an
                ``Agent()`` or ``Mcp()`` marker.
        """
        if self._declared is None:
            raise RuntimeError("CeleryAdapter.bind needs the recipe it declared; call recipe first")
        declared = self._declared
        use_cases = application.discovered.use_cases
        _reject_agent_markers(use_cases, application.kernel)
        _reject_mcp_markers(use_cases, application.kernel)

        async_runtime = _build_async_runtime(declared.celery.runtime)
        celery_app = create_celery_app(declared.celery)
        celery_app.set_current()
        _register_tasks(celery_app, declared, application, async_runtime, self._metrics)
        _connect_lifecycle_signals(_WorkerLifecycle(application, async_runtime))
        return celery_app


__all__ = ["CeleryAdapter"]
