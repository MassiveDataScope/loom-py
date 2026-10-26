"""SQL wiring for the composition root."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from loom.core.config import ConfigContext, ConfigKey
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql import (
    CallerBoundSql,
    NullSqlQueryService,
    SqlConfig,
    SqlExecutor,
    SqlQueryService,
)
from loom.core.sql.config import reject_retired_sql_keys
from loom.core.sql.loader import sql_island

if TYPE_CHECKING:
    from loom.core.sql.clickhouse import ClickHouseConnectionRegistry


@dataclass(frozen=True)
class SqlWiring:
    """Resolved SQL subsystem pieces for the auto-bootstrap REST app.

    Args:
        config: Parsed ``sql:`` section, or ``None`` when absent.
        registry: ClickHouse connection registry entered by the app lifespan,
            or ``None`` when no section is configured.
        service: Query service registered in the container — the null
            implementation when *config* is ``None`` (spec M5).
    """

    config: SqlConfig | None
    registry: ClickHouseConnectionRegistry | None
    service: SqlQueryService


class RegistryExecutors(Mapping[str, SqlExecutor]):
    """Lazy executor view over the ClickHouse connection registry.

    Lets ``create_app`` construct the SQL service and bind endpoints without
    opening any connection: executors resolve on first access, once the app
    lifespan has entered the registry.
    """

    def __init__(self, registry: ClickHouseConnectionRegistry, names: tuple[str, ...]) -> None:
        self._registry = registry
        self._names = names

    def __getitem__(self, name: str) -> SqlExecutor:
        if name not in self._names:
            raise KeyError(name)
        return self._registry.executor(name)

    def __contains__(self, name: object) -> bool:
        # Membership from config alone: never touches the registry, so the
        # service policy checks work outside the lifespan as well.
        return name in self._names

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)


def resolve_sql(ctx: ConfigContext) -> SqlWiring:
    """Load the optional ``sql:`` section into its registry and service.

    Absent section → no registry and the null service, keeping
    ``SqlQueryService`` always resolvable with an actionable error (spec M5).
    Present section → a registry whose connections only open when the
    application opens. Who serves the SQL endpoints checks their
    authentication; this function knows no transport.
    """
    reject_retired_sql_keys(ctx.raw(ConfigKey.SQL))
    sql_cfg = ctx.section_optional(ConfigKey.SQL, SqlConfig)
    if sql_cfg is None:
        return SqlWiring(config=None, registry=None, service=NullSqlQueryService())
    registry = build_sql_registry(sql_cfg)
    executors = RegistryExecutors(registry, tuple(sql_cfg.connections))
    service = SqlQueryService(executors=executors, config=sql_cfg)
    return SqlWiring(config=sql_cfg, registry=registry, service=service)


def build_sql_registry(sql_cfg: SqlConfig) -> ClickHouseConnectionRegistry:
    """Construct the ClickHouse registry — no connection is opened here."""
    return sql_island().ClickHouseConnectionRegistry(config=sql_cfg)


def register_sql_collaborators(
    container: LoomContainer, sql: SqlWiring, observability: ObservabilityRuntime
) -> None:
    """Register both SQL collaborators (APPLICATION scope) — always present (M5).

    ``SqlQueryService`` is the unbound path, for system work with no caller;
    ``CallerBoundSql`` derives the roles from the verified identity and is what
    a use case acting on behalf of a caller injects. Both are always
    resolvable, so neither choice depends on the config being present.

    The runtime is handed to the bound path so its queries leave the same audit
    trail as the REST endpoint: one span per query, labelled with the effective
    roles and the caller subject.
    """
    service = sql.service
    config = sql.config if sql.config is not None else SqlConfig(connections={})
    caller_bound = CallerBoundSql(service, config, observability)
    container.register(SqlQueryService, lambda: service, scope=Scope.APPLICATION)
    container.register(CallerBoundSql, lambda: caller_bound, scope=Scope.APPLICATION)
