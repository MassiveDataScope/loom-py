"""The ``mongo`` persistence backend.

``pymongo`` is imported at module level: the persistence registry turns an
``ImportError`` at entry-point load into a ``ConfigError`` naming the
``loom-kernel[mongo]`` extra, and ``loom.core.repository.mongo`` itself stays
importable without the driver.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any, ClassVar, Literal

import msgspec
from pymongo import AsyncMongoClient

from loom.core.config import ConfigContext, ConfigError
from loom.core.config.keys import ConfigKey
from loom.core.model import BaseModel
from loom.core.persistence.abc import PersistenceWiring
from loom.core.persistence.profile import BackendRuntime
from loom.core.repository.abc import CursorCodec
from loom.core.repository.mongo.ids import (
    IdPolicy,
    ObjectIdPolicy,
    Uuid4IdPolicy,
    validate_id_field,
)
from loom.core.repository.mongo.registry import build_mongo_repository_registration_module
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import MongoUnitOfWorkFactory, active_session

_SECTION = ConfigKey.PERSISTENCE_MONGO.value

_logger = logging.getLogger(__name__)

_ID_POLICIES: dict[str, type[IdPolicy]] = {"uuid4": Uuid4IdPolicy, "objectid": ObjectIdPolicy}


class _MongoConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    uri: str
    database: str
    transactions: bool = False
    collections: dict[str, str] = {}
    id: Literal["uuid4", "objectid"] = "uuid4"
    max_pool_size: int | None = None
    server_selection_timeout_ms: int | None = None


class MongoBackend:
    """Backend for ``persistence.backend: mongo``.

    Reads the ``persistence.mongo`` and ``persistence.cursors`` sections,
    opens one async client shared by the unit of work and every repository,
    binds each model to a collection of the configured database and closes
    the client at shutdown.
    """

    name: ClassVar[str] = "mongo"

    def build(
        self,
        ctx: ConfigContext,
        models: Sequence[type[BaseModel]],
        runtime: BackendRuntime,
    ) -> PersistenceWiring:
        """Build the Mongo wiring for the discovered models.

        Args:
            ctx: Configuration context holding the ``persistence.mongo`` section.
            models: Models whose repositories the DI module registers.
            runtime: Entry-point defaults; the Mongo client takes none of them yet.

        Returns:
            The Mongo wiring.

        Raises:
            ConfigError: When the ``persistence.mongo`` section is missing, or
                ``persistence.cursors`` cannot build a cursor codec.
        """
        mongo_cfg = ctx.section_optional(_SECTION, _MongoConfig)
        if mongo_cfg is None:
            raise ConfigError(
                f"persistence.backend is {self.name!r} but the {_SECTION!r} "
                "section (uri, database) is missing."
            )
        cursor_codec = CursorCodec.from_context(ctx)
        client = _build_mongo_client(mongo_cfg)
        return PersistenceWiring(
            uow_factory=_unit_of_work_factory(client, mongo_cfg),
            repo_registration_module=build_mongo_repository_registration_module(
                client[mongo_cfg.database],
                models,
                collections=mongo_cfg.collections,
                id_policy=_ID_POLICIES[mongo_cfg.id](),
                session_provider=active_session,
                cursor_codec=cursor_codec,
            ),
            default_repository_type=RepositoryMongo,
            lifespan_init=lambda: _lifespan(client),
            prepare_models=_prepare_models,
            readiness=lambda: _readiness(client),
        )


def _build_mongo_client(mongo_cfg: _MongoConfig) -> AsyncMongoClient[Any]:
    """Construct the async client; only the options set in config are forwarded.

    The client is ``tz_aware`` so datetimes read back as aware UTC, the form
    the repository writes them in.
    """
    options: dict[str, Any] = {"tz_aware": True}
    if mongo_cfg.max_pool_size is not None:
        options["maxPoolSize"] = mongo_cfg.max_pool_size
    if mongo_cfg.server_selection_timeout_ms is not None:
        options["serverSelectionTimeoutMS"] = mongo_cfg.server_selection_timeout_ms
    return AsyncMongoClient(mongo_cfg.uri, **options)


def _unit_of_work_factory(
    client: AsyncMongoClient[Any], mongo_cfg: _MongoConfig
) -> MongoUnitOfWorkFactory:
    _logger.info("mongo unit of work: transactions=%s", mongo_cfg.transactions)
    if mongo_cfg.transactions:
        return MongoUnitOfWorkFactory.transactional(client)
    return MongoUnitOfWorkFactory.without_transactions()


def _prepare_models(models: Sequence[type[BaseModel]]) -> None:
    """Reject, naming the model, any primary key Mongo cannot serve."""
    for model in models:
        validate_id_field(model)


async def _readiness(client: AsyncMongoClient[Any]) -> bool:
    """Probe the server with ``ping``.

    Any failure is logged with its traceback and reported as not ready: the
    probe exists to be answered, never to raise.
    """
    try:
        await client.admin.command("ping")
    except Exception:
        _logger.warning("mongo readiness probe failed", exc_info=True)
        return False
    return True


@asynccontextmanager
async def _lifespan(client: AsyncMongoClient[Any]) -> AsyncIterator[None]:
    try:
        yield
    finally:
        await client.close()


__all__ = ["MongoBackend"]
