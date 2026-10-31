"""The ``dynamodb`` persistence backend."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import Any, ClassVar

from loom.core.config import ConfigContext, reject_retired_keys
from loom.core.connections.aws import AwsClientConfig
from loom.core.connections.aws.client import build_aws_client
from loom.core.model import BaseModel
from loom.core.persistence.abc import PersistenceWiring
from loom.core.persistence.profile import BackendRuntime
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository.dynamodb.registry import (
    build_dynamodb_repository_registration_module,
)
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB, run_in_executor
from loom.core.repository.dynamodb.uow import DynamoUnitOfWorkFactory

_READY_TABLE_STATUSES = frozenset({"ACTIVE", "UPDATING"})

_logger = logging.getLogger(__name__)


class DynamoDBRepositoryConfig(AwsClientConfig, frozen=True, kw_only=True):
    """A ``dynamodb`` store section: the AWS client settings plus the table backing every model.

    Every key of :class:`~loom.core.connections.AwsClientConfig` applies, with
    the same defaults as anywhere else loom calls AWS; this context only adds
    *table* and requires *region*.

    Args:
        table: Table backing every model.

    Raises:
        ValueError: When *region* is not set: the table lives in one region.
    """

    table: str

    def __post_init__(self) -> None:
        if self.region is None:
            raise ValueError("region is required: the table lives in one region")


class DynamoDBBackend:
    """Backend for ``stores.default.backend: dynamodb``.

    Reads the ``stores.default`` section and binds every model to the
    configured table through one shared boto3 client. Models need no
    compilation and startup allocates no shared resource.
    """

    name: ClassVar[str] = "dynamodb"
    repository_type: ClassVar[type[Any] | None] = RepositoryDynamoDB

    def build(
        self,
        ctx: ConfigContext,
        models: Sequence[type[BaseModel]],
        runtime: BackendRuntime,
    ) -> PersistenceWiring:
        """Build the DynamoDB wiring for the discovered models.

        Args:
            ctx: Ignored; the store's section comes from ``runtime.catalog``.
            models: Models whose repositories the DI module registers.
            runtime: The catalog holding ``stores.default``; the DynamoDB client
                takes none of the entry-point defaults yet.

        Returns:
            The DynamoDB wiring.

        Raises:
            ConfigError: When the ``stores.default`` section is missing, names
                another backend, holds a retired key or is invalid.
        """
        spec = runtime.catalog.require(
            DEFAULT_STORE, backend=self.name, purpose=f"the {self.name!r} backend"
        )
        reject_retired_keys(spec.settings(), spec.path, DynamoDBRepositoryConfig)
        dynamo_cfg = spec.decode(DynamoDBRepositoryConfig)
        # The low-level client, not the resource: it is thread-safe, and
        # repository calls share it across the executor's threads.
        client = build_aws_client("dynamodb", dynamo_cfg)
        # One pool of threads per backend, as wide as the client's connection
        # pool: the default executor would cap parallel calls at cpu + 4.
        executor = ThreadPoolExecutor(
            max_workers=dynamo_cfg.pool.size, thread_name_prefix="loom-dynamodb"
        )
        return PersistenceWiring(
            uow_factory=DynamoUnitOfWorkFactory(),
            repo_registration_module=build_dynamodb_repository_registration_module(
                client, dynamo_cfg.table, models, executor=executor
            ),
            readiness=lambda: _readiness(client, dynamo_cfg.table, executor),
        )


async def _readiness(client: Any, table: str, executor: Executor) -> bool:
    """Probe the configured table with ``DescribeTable``.

    Ready when the table is ``ACTIVE`` or ``UPDATING`` (a table serves reads
    and writes during an index backfill). Any failure is logged with its
    traceback and reported as not ready: the probe exists to be answered,
    never to raise.
    """
    try:
        response = await run_in_executor(executor, client.describe_table, TableName=table)
        status = response["Table"]["TableStatus"]
    except Exception:
        _logger.warning("dynamodb readiness probe failed for table %r", table, exc_info=True)
        return False
    return status in _READY_TABLE_STATUSES


__all__ = ["DynamoDBBackend", "DynamoDBRepositoryConfig"]
