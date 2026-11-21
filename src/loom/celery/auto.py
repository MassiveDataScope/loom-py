"""Automatic Celery app creation from YAML configuration."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from celery import Celery  # type: ignore[import-untyped]

from loom.celery.bootstrap import bootstrap_worker

if TYPE_CHECKING:
    from loom.core.config import ConfigResolver
    from loom.core.engine.execution_observer import ExecutionObserver
    from loom.core.environment import Environment
    from loom.core.job.job import Job


def create_app(
    *config_paths: str,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    jobs: Sequence[type[Job[Any]]] = (),
    execution_observer: ExecutionObserver | None = None,
    resolvers: Sequence[ConfigResolver] = (),
) -> Celery:
    """Create a Celery app with all Loom job tasks registered.

    Args:
        *config_paths: YAML config files merged left-to-right. Mutually
            exclusive with *config_dir*.
        config_dir: Directory holding ``base.yaml`` and the per-environment
            overlays; ``<environment>.yaml`` loads after the base when present.
            With no *config_paths* and no *config_dir*, ``LOOM_CONFIG_DIR`` or
            ``./config``.
        environment: Environment of the worker; overrides ``LOOM_ENV``, which
            defaults to production when unset.
        jobs: Concrete ``Job`` subclasses to register as Celery tasks.
            Optional when ``app.packages`` is configured.
        execution_observer: Optional execution observer.
        resolvers: Resolvers for ``${name:key}`` placeholders, registered
            before the built-in ``secrets`` and ``ssm`` defaults.  A resolver
            named like a default replaces it.

    Returns:
        Configured Celery application.
    """
    result = bootstrap_worker(
        *config_paths,
        config_dir=config_dir,
        environment=environment,
        jobs=jobs,
        execution_observer=execution_observer,
        resolvers=resolvers,
    )
    return result.celery_app
