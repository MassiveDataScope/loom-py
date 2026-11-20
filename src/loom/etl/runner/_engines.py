"""ETL engine loading for runner wiring."""

from __future__ import annotations

from typing import Any, Protocol, cast, runtime_checkable

from loom.core.plugins.entrypoints import EntryPointNotFoundError, load_entry_point
from loom.etl.lineage._config import LineageConfig
from loom.etl.lineage.sinks import LineageWriter
from loom.etl.ports.contracts import ClientCommandExecutor, SourceReader, TargetWriter
from loom.etl.storage._config import StorageConfig

_ENGINE_ENTRY_POINT_GROUP = "loom.etl.engines"


@runtime_checkable
class EtlEngine(Protocol):
    """Factory protocol for backend reader/writer pairs."""

    def create_backends(
        self,
        config: StorageConfig,
        spark: Any = None,
    ) -> tuple[SourceReader, TargetWriter]:
        """Create backend reader/writer pair for one engine."""
        ...

    def create_lineage_writer(
        self,
        config: StorageConfig,
        lineage: LineageConfig,
        spark: Any = None,
    ) -> LineageWriter:
        """Create lineage writer for ETL lineage persistence."""
        ...

    def create_client_executor(
        self,
        config: StorageConfig,
        spark: Any = None,
    ) -> ClientCommandExecutor | None:
        """Create a client command executor for ``ClientStep`` execution.

        Args:
            config: Resolved storage config.
            spark: Active SparkSession, when the engine is Spark.

        Returns:
            An executor when the engine and config support client steps,
            or ``None`` when no client backend is configured.
        """
        ...


def load_etl_engine(engine: str) -> EtlEngine:
    """Load an ETL engine from package entry points.

    Args:
        engine: ``storage.engine`` value naming the engine distribution.

    Returns:
        An engine built from the registered class.

    Raises:
        ValueError: When no ETL engine is registered for ``engine``; the message
            names the engines that are.
    """
    provider_cls = cast(type[EtlEngine], _load_backend_class(engine))
    return provider_cls()


def _load_backend_class(engine: str) -> object:
    """Load the registered engine class, naming the alternatives on a miss."""
    try:
        return load_entry_point(_ENGINE_ENTRY_POINT_GROUP, engine, on_duplicate="warn_first")
    except EntryPointNotFoundError as exc:
        available = ", ".join(exc.available) if exc.available else "none"
        raise ValueError(
            f"Unsupported storage.engine={engine!r}. "
            f"No ETL engine registered in entry point group "
            f"'{_ENGINE_ENTRY_POINT_GROUP}'. Registered engines: {available}."
        ) from exc


__all__ = ["EtlEngine", "load_etl_engine"]
