"""Worker bootstrap for Celery-backed Job execution.

:func:`bootstrap_worker` is the shortcut that starts a Loom Celery worker: it
loads the configuration, composes the application with
:class:`~loom.celery.runtime.CeleryAdapter` through the same recipe the API
runs (:meth:`loom.composition.Project.compose`), and returns the Celery
app with every job and callback task registered.

Fork-safety design
------------------
Composing opens nothing: building a backend creates its engine, not a
connection.  ``worker_init`` runs the startup checks once in the parent,
which leave no connection open, and each prefork child opens the
application on its own async bridge in ``worker_process_init``.  See
:mod:`loom.celery.runtime` for the signal table.

Typical YAML layout (stores optional for pure in-memory jobs)::

    celery:
      broker_url: "redis://redis:6379/0"
      result_backend: "redis://redis:6379/1"
      runtime:
        backend: "asyncio"
        use_uvloop: true

    stores:
      default:
        backend: sqlalchemy
        url: "postgresql+asyncpg://user:pass@db/mydb"  # optional

    jobs:
      RecalcPricesJob:
        queue: "prices.heavy"
        retries: 5
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from celery import Celery  # type: ignore[import-untyped]

from loom.celery.runtime import CeleryAdapter
from loom.composition import Project
from loom.core.config import ConfigResolver
from loom.core.di.container import LoomContainer
from loom.core.environment import Environment
from loom.core.use_case.factory import UseCaseFactory

if TYPE_CHECKING:
    from loom.core.engine.compilable import Compilable
    from loom.core.engine.metrics import MetricsRecorder
    from loom.core.job.job import Job


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


def bootstrap_worker(
    *config_paths: str,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    jobs: Sequence[type[Job[Any]]] = (),
    use_cases: Sequence[type[Compilable]] = (),
    interfaces: Sequence[type[Any]] = (),
    metrics: MetricsRecorder | None = None,
    resolvers: Sequence[ConfigResolver] = (),
) -> WorkerBootstrapResult:
    """Bootstrap a Celery worker process and register all Job tasks.

    Resolves the environment once (*environment*, else ``LOOM_ENV``, else
    production; see :mod:`loom.core.environment`), loads configuration from
    ``config_dir`` or ``config_paths`` and composes the worker with
    :class:`~loom.celery.runtime.CeleryAdapter`: discovery, deploy checks
    (except the ``loom.rest`` ones: a worker serves no HTTP), persistence,
    kernel, ``JobService`` and ``SqlQueryService``, then the Celery tasks (one
    per job, and one per discovered ``JobCallback`` subclass and outcome) and
    the lifecycle signals. The environment is registered in the container.

    Persistence note: ``stores.default.backend`` selects the backend, with
    row-level security bound exactly as in the API for ``sqlalchemy``;
    without a ``stores.default`` section the worker selects ``none``,
    suitable for pure jobs that use no repository.

    Args:
        *config_paths: Paths to YAML configuration files, merged
            left-to-right; later files override earlier ones. Mutually
            exclusive with *config_dir*.
        config_dir: Local directory holding ``base.yaml`` and the
            per-environment overlays: ``base.yaml`` loads first, then
            ``<environment>.yaml`` when present. Either way the
            ``LOOM__SECTION__KEY`` variables merge last. With no
            *config_paths* and no *config_dir*, ``LOOM_CONFIG_DIR`` or ``./config``.
        environment: Environment of the worker; overrides ``LOOM_ENV``.
        jobs: Concrete ``Job`` subclasses to compile and register. When
            given they replace the jobs the scan of ``app.packages`` finds;
            when omitted, those are served.
        use_cases: ``UseCase`` subclasses to compile alongside the jobs, in
            addition to the ones the scan of ``app.packages`` finds. Needed
            only for use cases outside those packages that callbacks invoke
            through :class:`~loom.core.use_case.invoker.ApplicationInvoker`
            (e.g. ``app.entity(Product).get(...)``), so that their keys are
            registered in the worker process.
        interfaces: ``RestInterface`` subclasses whose route use-cases
            should be compiled in the worker.  Equivalent to passing the
            same classes via ``use_cases=`` but without having to enumerate
            individual use-case types — useful when callbacks interact with
            AutoCRUD-generated use-cases that are not importable by name.
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
        RuntimeError: If no ``app.packages`` config is provided and no
            ``jobs`` are declared.
        AgentCompilationError: When a compiled use case declares an
            ``Agent()`` or ``Mcp()`` marker.

    Example::

        result = bootstrap_worker(
            "config/base.yaml",
            "config/worker.yaml",
            jobs=[SendEmailJob, RecalcPricesJob],
            interfaces=[ProductInterface],
        )
        result.celery_app.start()
    """
    project = Project.load(
        *config_paths,
        config_dir=config_dir,
        environment=environment,
        code_path=None,
        resolvers=resolvers,
        caller="bootstrap_worker",
    )
    adapter = CeleryAdapter(
        jobs=jobs,
        use_cases=use_cases,
        interfaces=interfaces,
        metrics=metrics,
    )
    application = project.compose(adapter)
    celery_app = adapter.bind(application)
    return WorkerBootstrapResult(
        container=application.kernel.container,
        factory=application.kernel.factory,
        celery_app=celery_app,
    )


__all__ = ["WorkerBootstrapResult", "bootstrap_worker"]
