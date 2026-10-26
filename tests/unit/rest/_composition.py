"""Compose the FastAPI entry point from a configuration held in memory."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loom.composition import AppConfig, CompositionSource, compose_application
from loom.core.bootstrap import KernelRuntime
from loom.core.config import ConfigContext, ConfigKey
from loom.core.discovery.base import DiscoveryResult
from loom.core.environment import Environment
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import PersistenceWiring
from loom.rest.fastapi.adapter import FastAPIAdapter
from loom.rest.model import RestInterface


def composition_source(
    ctx: ConfigContext, *, environment: Environment = Environment.TEST
) -> CompositionSource:
    """What ``build_application`` would have resolved from files holding *ctx*."""
    return CompositionSource(
        config=ctx,
        environment=environment,
        code_path=Path.cwd(),
        config_root=Path.cwd(),
        app=ctx.section(ConfigKey.APP, AppConfig),
        observability=ObservabilityRuntime.noop(),
    )


def build_bootstrap(
    _app_cfg: object, ctx: ConfigContext, *, environment: Environment = Environment.TEST
) -> tuple[KernelRuntime, PersistenceWiring, DiscoveryResult, tuple[type[RestInterface[Any]], ...]]:
    """Compose as ``create_app`` does, returning what the removed ``_build_bootstrap`` did.

    The application config comes from *ctx*'s ``app`` section, as it does in
    ``create_app``; the first argument is accepted for the callers' sake.
    """
    application = compose_application(
        composition_source(ctx, environment=environment), FastAPIAdapter()
    )
    discovered = application.discovered
    return application.kernel, application.persistence, discovered, discovered.interfaces
