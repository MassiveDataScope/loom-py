"""Automatic Celery app creation from YAML configuration."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from celery import Celery  # type: ignore[import-untyped]

from loom.celery.bootstrap import bootstrap_worker
from loom.core.di.container import LoomContainer

if TYPE_CHECKING:
    from loom.core.config import ConfigResolver
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.environment import Environment
    from loom.core.job.job import Job


def create_app(
    *config_paths: str,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    jobs: Sequence[type[Job[Any]]] = (),
    callbacks: Sequence[type[Any]] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    metrics: MetricsAdapter | None = None,
    resolvers: Sequence[ConfigResolver] = (),
) -> Celery:
    """Create a Celery app with all Loom job tasks registered.

    Args:
        *config_paths: YAML config files merged left-to-right. Mutually
            exclusive with *config_dir*.
        config_dir: Directory holding ``base.yaml`` and the per-environment
            overlays; ``<environment>.yaml`` loads after the base when present.
        environment: Environment of the worker; overrides ``LOOM_ENV``, which
            defaults to production when unset.
        jobs: Concrete ``Job`` subclasses to register as Celery tasks.
            Optional when ``app.discovery`` (modules/manifest) is configured.
        callbacks: Optional callback classes for success/failure hooks.
        modules: Optional DI registration modules.
        metrics: Optional metrics adapter.
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
        callbacks=callbacks,
        modules=modules,
        metrics=metrics,
        resolvers=resolvers,
    )
    return result.celery_app
