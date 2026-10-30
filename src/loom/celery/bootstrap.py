"""Worker bootstrap for Celery-backed Job execution.

:func:`bootstrap_worker` is the shortcut that starts a Loom Celery worker: it
loads the configuration, composes the application with
:class:`~loom.celery.adapter.CeleryAdapter` through the same recipe the API
runs (:func:`loom.composition.compose_application`), and returns the Celery
app with every job and callback task registered.

Fork-safety design
------------------
Composing opens nothing: building a backend creates its engine, not a
connection.  ``worker_init`` runs the startup checks once in the parent,
which leave no connection open, and each prefork child opens the
application on its own async bridge in ``worker_process_init``.  See
:mod:`loom.celery.adapter` for the signal table.

Typical YAML layout (database optional for pure in-memory jobs)::

    celery:
      broker_url: "redis://redis:6379/0"
      result_backend: "redis://redis:6379/1"
      runtime:
        backend: "asyncio"
        use_uvloop: true

    database:
      url: "postgresql+asyncpg://user:pass@db/mydb"  # optional

    jobs:
      RecalcPricesJob:
        queue: "prices.heavy"
        retries: 5
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from celery import Celery  # type: ignore[import-untyped]

from loom.celery._discovery import WorkerManifest
from loom.celery.adapter import CeleryAdapter
from loom.composition import AppConfig, compose_application
from loom.composition.build import load_source
from loom.core.config import ConfigResolver
from loom.core.di.container import LoomContainer
from loom.core.environment import Environment
from loom.core.use_case.factory import UseCaseFactory

if TYPE_CHECKING:
    from loom.core.engine.compilable import Compilable
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job

_WORKER_NAME = "worker"


class _WorkerAppConfig(AppConfig, kw_only=True):
    """``app`` as a worker reads it: optional, and so is its name."""

    name: str = _WORKER_NAME


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkerBootstrapResult:
    """Result of a successful :func:`bootstrap_worker` call.

    Attributes:
        container: Validated DI container with all bindings resolved.
        factory: Factory for constructing Job instances per task execution.
        celery_app: Celery application with all Job tasks registered.

    Example::

        result = bootstrap_worker("config/worker.yaml", jobs=[SendEmailJob])
        result.celery_app.start()
    """

    container: LoomContainer
    factory: UseCaseFactory
    celery_app: Celery


# ---------------------------------------------------------------------------
# bootstrap_worker
# ---------------------------------------------------------------------------


def bootstrap_worker(
    *config_paths: str,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    jobs: Sequence[type[Job[Any]]] = (),
    use_cases: Sequence[type[Compilable]] = (),
    interfaces: Sequence[type[Any]] = (),
    callbacks: Sequence[type[Any]] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    metrics: MetricsAdapter | None = None,
    resolvers: Sequence[ConfigResolver] = (),
) -> WorkerBootstrapResult:
    """Bootstrap a Celery worker process and register all Job tasks.

    Resolves the environment once (*environment*, else ``LOOM_ENV``, else
    production; see :mod:`loom.core.environment`), loads configuration from
    ``config_dir`` or ``config_paths`` and composes the worker with
    :class:`~loom.celery.adapter.CeleryAdapter`: discovery, deploy checks
    (except the ``loom.rest`` ones: a worker serves no HTTP), persistence,
    kernel, ``JobService`` and ``SqlQueryService``, then the Celery tasks and
    the lifecycle signals. The environment is registered in the container.

    Persistence note: ``persistence.backend`` selects the backend; without
    it a ``database`` section of the worker's own selects ``sqlalchemy``,
    with row-level security bound exactly as in the API, and its absence
    selects ``none``, suitable for pure jobs that use no repository.

    Args:
        *config_paths: Paths to YAML configuration files, merged
            left-to-right; later files override earlier ones. Mutually
            exclusive with *config_dir*.
        config_dir: Local directory holding ``base.yaml`` and the
            per-environment overlays: ``base.yaml`` loads first, then
            ``<environment>.yaml`` when present. Either way the
            ``LOOM__SECTION__KEY`` variables merge last.
        environment: Environment of the worker; overrides ``LOOM_ENV``.
        jobs: Concrete ``Job`` subclasses to compile/register.  When
            omitted, jobs are discovered from ``app.discovery`` using
            mode ``modules`` or ``manifest``.
        use_cases: Additional ``UseCase`` subclasses to compile alongside
            ``jobs``.  Required when callbacks invoke
            :class:`~loom.core.use_case.invoker.ApplicationInvoker`
            (e.g. ``app.entity(Product).get(...)``), so that the
            corresponding use-case keys are registered in the worker
            process.  Ignored when jobs are discovered via ``app.discovery``
            (discovery already includes all compiled use-cases).
        interfaces: ``RestInterface`` subclasses whose route use-cases
            should be compiled in the worker.  Equivalent to passing the
            same classes via ``use_cases=`` but without having to enumerate
            individual use-case types — useful when callbacks interact with
            AutoCRUD-generated use-cases that are not importable by name.
        callbacks: Concrete ``JobCallback`` subclasses to register as
            Celery callback (success / error) tasks.
        modules: Callables that receive the container and register
            infrastructure bindings (repositories, services, etc.).
            Executed in declaration order before compilation.
        metrics: Optional metrics adapter forwarded to the compiler and
            executor.
        resolvers: Resolvers for ``${name:key}`` placeholders, registered
            before the built-in ``secrets`` and ``ssm`` defaults.  A resolver
            named like a default replaces it.

    Returns:
        :class:`WorkerBootstrapResult` with the container, factory, and
        configured Celery application.

    Raises:
        ConfigError: If neither or both of *config_paths* and *config_dir*
            are given, the environment is unknown, or a required
            configuration section is missing or fails validation.
        DeployCheckError: When a deploy check of level error fails in
            production or staging.
        RuntimeError: If no ``app.discovery`` config is provided and no
            ``jobs`` are declared.
        AgentCompilationError: When a compiled use case declares an
            ``Agent()`` or ``Mcp()`` marker.

    Example::

        result = bootstrap_worker(
            "config/base.yaml",
            "config/worker.yaml",
            jobs=[SendEmailJob, RecalcPricesJob],
            interfaces=[ProductInterface],
            callbacks=[EmailSentCallback],
        )
        result.celery_app.start()
    """
    source = load_source(
        *config_paths,
        config_dir=config_dir,
        environment=environment,
        code_path=None,
        resolvers=resolvers,
        caller="bootstrap_worker",
        default_app=_WorkerAppConfig(),
    )
    adapter = CeleryAdapter(
        jobs=jobs,
        use_cases=use_cases,
        interfaces=interfaces,
        callbacks=callbacks,
        metrics=metrics,
    )
    application = compose_application(source, adapter, modules=modules)
    celery_app = adapter.bind(application)
    return WorkerBootstrapResult(
        container=application.kernel.container,
        factory=application.kernel.factory,
        celery_app=celery_app,
    )


__all__ = ["WorkerBootstrapResult", "WorkerManifest", "bootstrap_worker"]
