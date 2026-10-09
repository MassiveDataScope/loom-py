"""Runtime wiring helpers for ETL runner dependencies.

Internal module — not part of the public API.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Final, Protocol, cast

from loom.core.config import ConfigContext, ConfigKey
from loom.core.plugins.optional import import_optional
from loom.etl.checkpoint import CheckpointStore, FsspecTempCleaner, TempCleaner
from loom.etl.checkpoint._backends._polars import _PolarsCheckpointBackend
from loom.etl.checkpoint._backends._spark import _SparkCheckpointBackend
from loom.etl.checkpoint._cleaners import _is_cloud_path
from loom.etl.checkpoint._options import encryption_options
from loom.etl.lineage._config import LineageConfig
from loom.etl.lineage.sinks import LineageStore, LineageWriter, TableLineageStore
from loom.etl.runner._agents import UnservedAgents
from loom.etl.runner._providers import load_backend_provider
from loom.etl.runtime.contracts import (
    AgentBatchRunner,
    ClientCommandExecutor,
    SourceReader,
    TargetWriter,
)
from loom.etl.storage._config import StorageConfig, StorageEngine

_log = logging.getLogger(__name__)

_AGENTS_ISLAND: Final = "loom.ai.etl"


class _CheckpointConfig(Protocol):
    """Structural protocol satisfied by every StorageConfig variant."""

    @property
    def checkpoint_root(self) -> str: ...

    @property
    def checkpoint_storage_options(self) -> dict[str, str]: ...


def make_backends(
    config: StorageConfig,
    spark: Any = None,
) -> tuple[SourceReader, TargetWriter]:
    """Instantiate reader and writer from *config*.

    Args:
        config: Resolved storage config.
        spark: Active SparkSession. Required for Unity Catalog.

    Returns:
        Pair ``(reader, writer)``.

    Selection rule:
        * ``spark is not None`` -> Spark backends.
        * ``spark is None`` -> Polars backends.

    Raises:
        ValueError: If ``storage.engine='spark'`` but ``spark`` is not provided.
    """
    if config.engine == "spark" and spark is None:
        raise ValueError(
            "A SparkSession is required when storage.engine='spark'. "
            "Pass spark=<session> to ETLRunner.from_yaml() or ETLRunner.from_config()."
        )
    engine = _resolve_engine(config, spark)
    provider = load_backend_provider(engine)
    return provider.create_backends(config, spark)


def make_checkpoint_store(
    config: _CheckpointConfig,
    spark: Any = None,
    cleaner: TempCleaner | None = None,
) -> CheckpointStore | None:
    """Build a checkpoint store from config or return ``None`` when disabled."""
    if not config.checkpoint_root:
        return None
    if not _is_cloud_path(config.checkpoint_root):
        raise ValueError(
            "checkpoint_root must be a cloud URI (s3://, gs://, abfss://, ...). "
            "Local checkpoint paths are not supported."
        )
    resolved_cleaner = cleaner or FsspecTempCleaner(
        storage_options=config.checkpoint_storage_options or {}
    )
    backend = _make_checkpoint_backend(spark, config.checkpoint_storage_options or {})
    return CheckpointStore(
        root=config.checkpoint_root,
        backend=backend,
        cleaner=resolved_cleaner,
    )


def make_lineage_writer(
    storage: StorageConfig,
    lineage: LineageConfig,
    spark: Any = None,
) -> LineageWriter | None:
    """Build a lineage writer from storage/lineage config."""
    if not lineage.enabled:
        return None
    lineage.validate()
    engine = _resolve_engine(storage, spark)
    provider = load_backend_provider(engine)
    return provider.create_lineage_writer(storage, lineage, spark)


def make_lineage_store(
    storage: StorageConfig,
    lineage: LineageConfig,
    spark: Any = None,
) -> LineageStore | None:
    """Build a lineage store from storage/lineage config."""
    if not lineage.enabled:
        return None
    lineage.validate()
    engine = _resolve_engine(storage, spark)
    provider = load_backend_provider(engine)
    writer = provider.create_lineage_writer(storage, lineage, spark)
    if lineage.database:
        return TableLineageStore(writer, database=lineage.database)
    return TableLineageStore(writer, database="")


def make_client_executor(
    config: StorageConfig,
    spark: Any = None,
) -> ClientCommandExecutor | None:
    """Build a client command executor from config or return ``None``.

    Args:
        config: Resolved storage config.
        spark: Active SparkSession. Required for the Spark engine.

    Returns:
        An executor when the engine and config support client steps,
        or ``None`` when no client backend is configured.
    """
    engine = _resolve_engine(config, spark)
    provider = load_backend_provider(engine)
    return provider.create_client_executor(config, spark)


class _AgentsIsland(Protocol):
    """What :data:`_AGENTS_ISLAND` publishes."""

    def agent_runner(self, context: ConfigContext, *, root: Path) -> AgentBatchRunner: ...


def make_agent_runner(
    context: ConfigContext, root: Path, *, config: StorageConfig, spark: Any = None
) -> AgentBatchRunner | None:
    """Build the agent runner of the config's ``ai:`` section, or return ``None``.

    The AI pillar is loaded by module path only when the section is present
    and the engine is Polars, so a pipeline without agents never imports it.
    On Spark the runner refuses every ``WithAgent`` at compile time.

    Args:
        context: Config the runner was built from.
        root: Directory holding the YAML; ``ai.root`` resolves against it.
        config: Resolved storage config, which decides the engine.
        spark: Active SparkSession, when one is given.

    Returns:
        The runner, or ``None`` when the config declares no ``ai:`` section.

    Raises:
        MissingExtraError: When the ``etl-polars`` extra is not installed.
    """
    if not context.has(ConfigKey.AI):
        return None
    engine = _resolve_engine(config, spark)
    if engine != StorageEngine.POLARS:
        return UnservedAgents(engine)
    island = cast(_AgentsIsland, import_optional(_AGENTS_ISLAND, extra="etl-polars"))
    return island.agent_runner(context, root=root)


def agents_for_engine(
    agents: AgentBatchRunner | None, config: StorageConfig, spark: Any = None
) -> AgentBatchRunner | None:
    """Return *agents*, or a runner refusing every ``WithAgent`` when the engine is not Polars.

    Args:
        agents: Agent runner given to the ETL runner, if any.
        config: Resolved storage config, which decides the engine.
        spark: Active SparkSession, when one is given.

    Returns:
        *agents* on Polars or when it is ``None``; otherwise the refusing runner.
    """
    engine = _resolve_engine(config, spark)
    if agents is None or engine == StorageEngine.POLARS:
        return agents
    return UnservedAgents(engine)


def _make_checkpoint_backend(spark: Any, storage_options: dict[str, str]) -> Any:
    if spark is not None:
        encryption = encryption_options(storage_options)
        if encryption:
            # Spark writes checkpoints through Hadoop's S3A, which reads SSE from
            # its own conf (fs.s3a.server-side-encryption-*); loom cannot inject it.
            _log.warning(
                "checkpoint encryption options ignored by the Spark backend keys=%s; "
                "set fs.s3a.server-side-encryption-algorithm / -key on the Spark session",
                sorted(encryption),
            )
        return _SparkCheckpointBackend(spark)
    return _PolarsCheckpointBackend(storage_options)


def _resolve_engine(config: StorageConfig, spark: Any) -> str:
    if spark is not None:
        return "spark"
    return config.engine


__all__ = [
    "make_backends",
    "make_checkpoint_store",
    "make_agent_runner",
    "agents_for_engine",
    "make_client_executor",
    "make_lineage_writer",
    "make_lineage_store",
]
