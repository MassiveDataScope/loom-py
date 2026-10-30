"""Compose the FastAPI entry point from a configuration held in memory."""

from __future__ import annotations

from typing import Any

from loom.composition import compose_application
from loom.core.bootstrap import KernelRuntime
from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
from loom.core.environment import Environment
from loom.core.persistence import PersistenceWiring
from loom.rest.fastapi.adapter import FastAPIAdapter
from loom.rest.model import RestInterface
from tests.helpers.composition import composition_source


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
