"""The ``dynamodb`` persistence backend."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import Any, ClassVar

import msgspec

from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel
from loom.core.persistence.abc import PersistenceWiring
from loom.core.repository.dynamodb.registry import (
    build_dynamodb_repository_registration_module,
)
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB
from loom.core.repository.dynamodb.uow import DynamoUnitOfWorkFactory

_SECTION = "persistence.dynamodb"
_READY_TABLE_STATUSES = frozenset({"ACTIVE", "UPDATING"})

_logger = logging.getLogger(__name__)


class _DynamoDBConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    region: str
    table: str
    endpoint_url: str | None = None
    max_pool_connections: int = 32


class DynamoDBBackend:
    """Backend for ``persistence.backend: dynamodb``.

    Reads the ``persistence.dynamodb`` section and binds every model to the
    configured table through one shared boto3 client. Models need no
    compilation and startup allocates no shared resource.
    """

    name: ClassVar[str] = "dynamodb"

    def build(self, ctx: ConfigContext, models: Sequence[type[BaseModel]]) -> PersistenceWiring:
        """Build the DynamoDB wiring for the discovered models.

        Args:
            ctx: Configuration context holding the ``persistence.dynamodb`` section.
            models: Models whose repositories the DI module registers.

        Returns:
            The DynamoDB wiring.

        Raises:
            ConfigError: When the ``persistence.dynamodb`` section is missing.
        """
        dynamo_cfg = ctx.section_optional(_SECTION, _DynamoDBConfig)
        if dynamo_cfg is None:
            raise ConfigError(
                f"persistence.backend is {self.name!r} but the {_SECTION!r} "
                "section (region, table) is missing."
            )
        client = _build_dynamodb_client(dynamo_cfg)
        return PersistenceWiring(
            uow_factory=DynamoUnitOfWorkFactory(),
            repo_registration_module=build_dynamodb_repository_registration_module(
                client, dynamo_cfg.table, models
            ),
            default_repository_type=RepositoryDynamoDB,
            readiness=lambda: _readiness(client, dynamo_cfg.table),
        )


def _build_dynamodb_client(dynamo_cfg: _DynamoDBConfig) -> Any:
    """Construct a boto3 low-level ``dynamodb`` client from config.

    The low-level client (not the resource) is used because it is thread-safe:
    repository operations run under ``asyncio.to_thread`` and share one client
    across worker threads. ``max_pool_connections`` sizes the underlying
    connection pool to that concurrency.

    Credentials are never taken from config: the client is created without
    explicit keys so boto3's default credential chain applies — the task role
    on ECS, or ``endpoint_url`` plus environment credentials against a local /
    fake DynamoDB in tests.
    """
    # Local import: boto3/botocore are optional dependencies (loom[dynamodb])
    # and the package must stay importable without them.
    import boto3  # type: ignore[import-untyped]
    from botocore.config import Config  # type: ignore[import-untyped]

    kwargs: dict[str, Any] = {
        "region_name": dynamo_cfg.region,
        "config": Config(max_pool_connections=dynamo_cfg.max_pool_connections),
    }
    if dynamo_cfg.endpoint_url is not None:
        kwargs["endpoint_url"] = dynamo_cfg.endpoint_url
    return boto3.client("dynamodb", **kwargs)


async def _readiness(client: Any, table: str) -> bool:
    """Probe the configured table with ``DescribeTable``.

    Ready when the table is ``ACTIVE`` or ``UPDATING`` (a table serves reads
    and writes during an index backfill). Any failure is logged with its
    traceback and reported as not ready: the probe exists to be answered,
    never to raise.
    """
    try:
        response = await asyncio.to_thread(client.describe_table, TableName=table)
        status = response["Table"]["TableStatus"]
    except Exception:
        _logger.warning("dynamodb readiness probe failed for table %r", table, exc_info=True)
        return False
    return status in _READY_TABLE_STATUSES


__all__ = ["DynamoDBBackend"]
