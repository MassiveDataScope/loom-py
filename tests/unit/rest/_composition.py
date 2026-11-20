"""Compose the FastAPI entry point from a configuration held in memory."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from loom.core.bootstrap import Kernel
from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
from loom.core.environment import Environment
from loom.core.stores.uow.abc import UnitOfWorkFactory
from loom.rest.discovery import interfaces_in
from loom.rest.fastapi.adapter import FastAPIAdapter
from loom.rest.model import RestInterface
from tests.helpers.composition import composition_project, uow_factory

SQLITE_STORE: dict[str, object] = {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"}
"""An in-memory SQLite default store."""


def dynamodb_store(table: str) -> dict[str, object]:
    """A DynamoDB default store on a local endpoint, serving *table*."""
    return {
        "backend": "dynamodb",
        "region": "eu-west-1",
        "table": table,
        "url": "http://localhost:8000",
    }


def store_context(store: Mapping[str, object] | None = None, **sections: object) -> ConfigContext:
    """An application ``demo`` on the default store *store*, SQLite when ``None``."""
    return ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "stores": {
                "default": dict(store or SQLITE_STORE),
                "settings": {"cursors": {"signing": "ephemeral"}},
            },
            **sections,
        }
    )


def build_bootstrap(
    _app_cfg: object, ctx: ConfigContext, *, environment: Environment = Environment.TEST
) -> tuple[
    Kernel,
    UnitOfWorkFactory | None,
    DiscoveryResult,
    tuple[type[RestInterface[Any]], ...],
]:
    """Compose as ``create_app`` does; return the kernel, unit-of-work factory and discovery.

    The factory is the default store's; the last item is the discovered interfaces.

    The application config comes from *ctx*'s ``app`` section, as it does in
    ``create_app``; the first argument is accepted for the callers' sake.
    """
    application = composition_project(ctx, environment=environment).compose(FastAPIAdapter())
    discovered = application.discovered
    return application.kernel, uow_factory(application), discovered, interfaces_in(discovered)
