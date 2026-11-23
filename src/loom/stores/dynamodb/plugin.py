"""The ``dynamodb`` store backend."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, ClassVar, ParamSpec, TypeVar

from loom.core.concurrency import run_in_executor
from loom.core.config import ConfigContext
from loom.core.connections.aws import AwsClientConfig
from loom.core.errors import InvalidValue
from loom.core.log_names import LogEvent, LogKey
from loom.core.model import BaseModel
from loom.core.stores.catalog import MODELS_ONLY, StoreCapabilities, StoreSpec
from loom.core.stores.plugin import StoreWiring
from loom.core.stores.profile import StorePluginContext
from loom.stores.dynamodb.registry import (
    build_dynamodb_repository_registration_module,
)
from loom.stores.dynamodb.repository import (
    DynamoDBRepository,
    refuse_business_key_without_hash,
)
from loom.stores.dynamodb.uow import DynamoDBUnitOfWorkFactory

_READY_TABLE_STATUSES = frozenset({"ACTIVE", "UPDATING"})

_P = ParamSpec("_P")
_T = TypeVar("_T")

_logger = logging.getLogger(__name__)


class DynamodbEvent(LogEvent):
    """The events the DynamoDB store logs."""

    READINESS_PROBE_FAILED = "dynamodb.readiness_probe_failed"


class DynamodbKey(LogKey):
    """The fields of a DynamoDB store record."""

    TABLE = "loom.dynamodb.table"


class DynamoDBRepositoryConfig(AwsClientConfig, frozen=True, kw_only=True):
    """A ``dynamodb`` store section: the AWS client settings plus the table backing every model.

    Every key of :class:`~loom.core.connections.AwsClientConfig` applies, with
    the same defaults as anywhere else loom calls AWS, and the store takes
    every key it does not set from ``aws:``; this context only adds *table*
    and requires *region*.

    Args:
        table: Table backing every model.

    Raises:
        InvalidValue: When *region* is not set: the table lives in one region.
    """

    table: str

    def __post_init__(self) -> None:
        if self.region is None:
            raise InvalidValue("region is required: the table lives in one region")


class DynamoDBStorePlugin:
    """Backend for ``stores.<name>.backend: dynamodb``.

    Reads the section of the store it serves and binds every model to the
    configured table through one shared boto3 client. Models need no
    compilation and startup allocates no shared resource.
    """

    name: ClassVar[str] = "dynamodb"
    repository_type: ClassVar[type[Any] | None] = DynamoDBRepository

    def capabilities(self, spec: StoreSpec) -> StoreCapabilities:
        """Describe DynamoDB: models, without rollback or cursor paging, whatever *spec* says."""
        del spec
        return MODELS_ONLY

    def build(
        self,
        ctx: ConfigContext,
        models: Sequence[type[BaseModel]],
        context: StorePluginContext,
    ) -> StoreWiring:
        """Build the DynamoDB wiring for the discovered models.

        Args:
            ctx: Ignored; the store's section comes from ``context.catalog``.
            models: Models whose repositories the DI module registers.
            context: The catalog and the store it serves (``context.store``);
                the DynamoDB client takes none of the entry-point defaults yet.

        Returns:
            The DynamoDB wiring.

        Raises:
            ConfigError: When the store's section or ``aws:`` is missing a
                required key, names another backend, holds a retired key or
                is invalid.
        """
        spec = context.catalog.served_by(self.name, context.store)
        del ctx
        dynamo_cfg = DynamoDBRepositoryConfig.from_config(
            spec.config, spec.path, own=spec.section()
        )
        client = context.aws.client("dynamodb", dynamo_cfg)
        executor = _ReopenableThreads(dynamo_cfg.pool.size)
        return StoreWiring(
            uow_factory=DynamoDBUnitOfWorkFactory(),
            prepare_models=_prepare_models,
            repo_registration_module=build_dynamodb_repository_registration_module(
                client, dynamo_cfg.table, models, store=spec.key, executor=executor
            ),
            lifespan_init=lambda: _lifespan(executor),
            readiness=lambda: _readiness(client, dynamo_cfg.table, executor),
        )


def _prepare_models(models: Sequence[type[BaseModel]]) -> None:
    """Reject, naming the model, any business key DynamoDB cannot keep unique."""
    for model in models:
        refuse_business_key_without_hash(model)


class _ReopenableThreads(Executor):
    """A thread pool that refuses work once shut down, until the next lifespan reopens it."""

    def __init__(self, size: int) -> None:
        self._size = size
        self._closed = False
        self._pool = self._new_pool()

    def _new_pool(self) -> ThreadPoolExecutor:
        return ThreadPoolExecutor(max_workers=self._size, thread_name_prefix="loom-dynamodb")

    def reopen(self) -> None:
        if self._closed:
            self._closed, self._pool = False, self._new_pool()

    def submit(self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs) -> Future[_T]:
        return self._pool.submit(fn, *args, **kwargs)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self._closed = True
        self._pool.shutdown(wait, cancel_futures=cancel_futures)


@asynccontextmanager
async def _lifespan(executor: _ReopenableThreads) -> AsyncIterator[None]:
    executor.reopen()
    try:
        yield
    finally:
        await asyncio.to_thread(executor.shutdown)


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
        _logger.warning(
            DynamodbEvent.READINESS_PROBE_FAILED,
            extra={DynamodbKey.TABLE: table},
            exc_info=True,
        )
        return False
    return status in _READY_TABLE_STATUSES


__all__ = ["DynamoDBStorePlugin", "DynamoDBRepositoryConfig"]
