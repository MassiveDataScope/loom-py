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

from pymongo import AsyncMongoClient

from loom.core.config import ConfigContext, reject_retired_keys
from loom.core.connections.mongo import MongoConnection
from loom.core.connections.mongo.client import build_async_mongo_client
from loom.core.model import BaseModel
from loom.core.persistence.abc import PersistenceWiring
from loom.core.persistence.profile import BackendRuntime
from loom.core.persistence.stores import DEFAULT_STORE, StoreCapabilities, StoreSpec
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

_logger = logging.getLogger(__name__)

_ID_POLICIES: dict[str, type[IdPolicy]] = {"uuid4": Uuid4IdPolicy, "objectid": ObjectIdPolicy}


class MongoRepositoryConfig(MongoConnection, frozen=True, kw_only=True):
    """A ``mongo`` store section: the Mongo connection plus how the repository uses it.

    Every key of :class:`~loom.core.connections.MongoConnection` applies, as
    in every other context reading MongoDB.

    Args:
        transactions: Run each unit of work in a multi-document transaction.
        collections: Collection name per model, overriding the derived one.
        id: Identifier policy of the documents.
    """

    transactions: bool = False
    collections: dict[str, str] = {}
    id: Literal["uuid4", "objectid"] = "uuid4"


class MongoBackend:
    """Backend for ``stores.default.backend: mongo``.

    Reads the ``stores.default`` and ``persistence.cursors`` sections,
    opens one async client shared by the unit of work and every repository,
    binds each model to a collection of the configured database and closes
    the client at shutdown.
    """

    name: ClassVar[str] = "mongo"
    repository_type: ClassVar[type[Any] | None] = RepositoryMongo

    def capabilities(self, spec: StoreSpec) -> StoreCapabilities:
        """Describe the store, resolving only its ``transactions`` key, never its connection."""
        transactional = spec.config.section_or_default(f"{spec.path}.transactions", bool, False)
        return StoreCapabilities(
            serves_models=True, transactional=transactional, pages_with_cursors=True
        )

    def build(
        self,
        ctx: ConfigContext,
        models: Sequence[type[BaseModel]],
        runtime: BackendRuntime,
    ) -> PersistenceWiring:
        """Build the Mongo wiring for the discovered models.

        Args:
            ctx: Configuration context holding the ``persistence.cursors`` section.
            models: Models whose repositories the DI module registers.
            runtime: The catalog holding ``stores.default``; the Mongo client
                takes none of the entry-point defaults yet.

        Returns:
            The Mongo wiring.

        Raises:
            ConfigError: When the ``stores.default`` section is missing, names
                another backend, holds a retired key or is invalid, or
                ``persistence.cursors`` cannot build a cursor codec.
        """
        spec = runtime.catalog.require(
            DEFAULT_STORE, backend=self.name, purpose=f"the {self.name!r} backend"
        )
        reject_retired_keys(spec.settings(), spec.path, MongoRepositoryConfig)
        mongo_cfg = spec.decode(MongoRepositoryConfig)
        cursor_codec = CursorCodec.from_context(ctx)
        # tz_aware: datetimes read back as aware UTC, the form the repository writes.
        client = build_async_mongo_client(mongo_cfg, tz_aware=True)
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
            lifespan_init=lambda: _lifespan(client),
            prepare_models=_prepare_models,
            readiness=lambda: _readiness(client),
        )


def _unit_of_work_factory(
    client: AsyncMongoClient[Any], mongo_cfg: MongoRepositoryConfig
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


__all__ = ["MongoBackend", "MongoRepositoryConfig"]
