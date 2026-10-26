"""ClickHouse connection registry: client lifecycle plus fail-closed startup checks.

The registry is an async context manager. Entering it creates one client per
configured connection, asserts per-query role support on the driver and runs
the sentinel-role startup probe (spec §2); leaving it closes every client.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from types import TracebackType
from typing import Any, Self

import msgspec

from loom.core.config.convert import convert_config
from loom.core.config.errors import ConfigError
from loom.core.connections.clickhouse.client import async_client_options
from loom.core.sql.abc import UnknownConnectionError
from loom.core.sql.clickhouse._client import (
    AsyncClickHouseClient,
    ClickHouseClientFactory,
    DatabaseError,
    create_driver_client,
    enable_repeated_query_params,
    sanitize_backend_error,
    supports_repeated_query_params,
)
from loom.core.sql.clickhouse.executor import ClickHouseSqlExecutor
from loom.core.sql.config import SqlConfig, SqlConnectionConfig, reject_retired_sql_keys

_logger = logging.getLogger(__name__)

_PROBE_SQL = "SELECT 1"
_SENTINEL_ROLE = "loom_probe_sentinel_role_does_not_exist"
# Anchored to the error-code marker so an incidental "511" elsewhere in the
# message can never satisfy the fail-closed probe. Case-insensitive to match
# both the server body ("Code: 511. DB::Exception ...") and the driver
# summary line ("Received ClickHouse exception, code: 511").
_UNKNOWN_ROLE_CODE_RE = re.compile(r"\bcode:\s*511\b", re.IGNORECASE)


class ClickHouseConnectionRegistry:
    """Owns the ClickHouse clients and executors for every named connection.

    Usable only as an async context manager: clients exist between
    ``__aenter__`` and ``__aexit__``, which eliminates any intermediate
    started/stopped state. Startup is fail-closed: a driver without per-query
    role support or a server that silently ignores roles aborts the enter and
    closes everything created so far.

    Args:
        config: Parsed ``sql:`` section with the named connections.
        client_factory: Factory receiving the exact ``get_async_client``
            keyword arguments. Defaults to the real driver factory.

    Example::

        async with ClickHouseConnectionRegistry(config=sql_config) as registry:
            executor = registry.executor("analytics")
    """

    def __init__(
        self,
        *,
        config: SqlConfig,
        client_factory: ClickHouseClientFactory | None = None,
    ) -> None:
        self._config = config
        self._client_factory = client_factory
        self._clients: dict[str, AsyncClickHouseClient] = {}
        self._executors: dict[str, ClickHouseSqlExecutor] = {}
        self._entered = False

    @classmethod
    def from_config(cls, raw: Mapping[str, Any]) -> Self:
        """Build a registry from the raw ``sql:`` section mapping.

        Args:
            raw: Mapping shaped like the ``sql:`` config section
                (``{"connections": {...}}``).

        Returns:
            A registry over the validated configuration.

        Raises:
            ConfigError: When the mapping holds a retired key or fails
                ``SqlConfig`` validation.
        """
        reject_retired_sql_keys(raw)
        try:
            config = convert_config(raw, SqlConfig, strict=False)
        except msgspec.ValidationError as exc:
            raise ConfigError(f"Invalid 'sql' config section: {exc}") from exc
        return cls(config=config)

    async def __aenter__(self) -> Self:
        """Create, verify and probe one client per configured connection.

        Raises:
            ImportError: When ``clickhouse-connect`` is missing and no custom
                factory was provided.
            ConfigError: When a client lacks per-query role support or the
                startup probe detects that roles are silently ignored.
        """
        factory = self._client_factory if self._client_factory is not None else create_driver_client
        try:
            for name, connection in self._config.connections.items():
                await self._open_connection(name, connection, factory)
        except BaseException:
            await self._aclose_clients()
            raise
        self._entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close every client created on enter."""
        self._entered = False
        await self._aclose_clients()

    def executor(self, name: str) -> ClickHouseSqlExecutor:
        """Return the executor of the connection *name*.

        Args:
            name: Configured connection name.

        Returns:
            The executor bound to that connection.

        Raises:
            RuntimeError: When the registry was not entered as a context.
            UnknownConnectionError: When *name* is not configured.
        """
        if not self._entered:
            raise RuntimeError(
                "ClickHouseConnectionRegistry must be entered before use: "
                "wrap it in 'async with registry:' to open the connections"
            )
        executor = self._executors.get(name)
        if executor is None:
            raise UnknownConnectionError(name)
        return executor

    async def _open_connection(
        self,
        name: str,
        connection: SqlConnectionConfig,
        factory: ClickHouseClientFactory,
    ) -> None:
        client = await factory(**_client_kwargs(connection))
        self._clients[name] = client
        _require_role_transport_setting(name, client)
        _require_multi_role_transport(name, connection)
        if _roles_configured(connection):
            await _probe_role_enforcement(name, client)
        self._executors[name] = ClickHouseSqlExecutor(client=client, config=connection)

    async def _aclose_clients(self) -> None:
        """Close every client in isolation: one failing close never leaks the rest."""
        for name, client in self._clients.items():
            try:
                await client.close()
            except Exception:
                _logger.warning(
                    "Failed to close ClickHouse client for connection %r", name, exc_info=True
                )
        self._clients.clear()
        self._executors.clear()


def _client_kwargs(connection: SqlConnectionConfig) -> dict[str, Any]:
    """Exact ``get_async_client`` keyword arguments for one connection."""
    extra: dict[str, Any] = {
        # Never rely on the driver's mutable global default (spec §2).
        "autogenerate_session_id": False,
    }
    if not connection.readonly:
        # Driver retries could re-run non-idempotent statements (re-INSERT).
        extra["query_retries"] = 0
    return async_client_options(connection, **extra)


def _roles_configured(connection: SqlConnectionConfig) -> bool:
    return bool(connection.allowed_roles) or connection.default_role is not None


def _require_role_transport_setting(name: str, client: AsyncClickHouseClient) -> None:
    if "role" in client.valid_transport_settings:
        return
    raise ConfigError(
        f"SQL connection {name!r}: the ClickHouse client does not accept 'role' "
        "as a per-query transport setting (requires clickhouse-connect >= 0.9.2)"
    )


def _require_multi_role_transport(name: str, connection: SqlConnectionConfig) -> None:
    """Fail-closed check for connections that can ever apply more than one role.

    ClickHouse only accepts several roles as repeated HTTP parameters. A driver
    that cannot emit them would send an invalid single role, so a multi-role
    allowlist aborts startup instead of failing at the first such query.
    """
    if len(connection.allowed_roles) <= 1:
        return
    # Enabled here and nowhere else: the driver workaround is a process-wide
    # mutation, so it only happens for an application that actually needs it.
    enable_repeated_query_params()
    if supports_repeated_query_params():
        return
    raise ConfigError(
        f"SQL connection {name!r}: 'allowed_roles' declares "
        f"{len(connection.allowed_roles)} roles but the installed "
        "clickhouse-connect cannot send one 'role' HTTP parameter per role, "
        "which is the only form ClickHouse accepts for multiple roles; "
        "aborting startup fail-closed"
    )


async def _probe_role_enforcement(name: str, client: AsyncClickHouseClient) -> None:
    """Fail-closed startup probe: a nonexistent sentinel role must be rejected.

    The probe issues a privilege-free ``SELECT 1`` carrying a sentinel role
    that does not exist. A healthy server rejects it with error code 511
    (UNKNOWN_ROLE); silent success means per-query roles are ignored and
    startup must abort.
    """
    try:
        await client.query(_PROBE_SQL, settings={"role": _SENTINEL_ROLE})
    except DatabaseError as exc:
        if _UNKNOWN_ROLE_CODE_RE.search(str(exc)):
            return
        raise ConfigError(
            f"SQL connection {name!r}: startup role probe failed unexpectedly: "
            f"{sanitize_backend_error(str(exc))}"
        ) from exc
    raise ConfigError(
        f"SQL connection {name!r}: the ClickHouse server accepted a nonexistent "
        "sentinel role, so per-query role enforcement is not active (requires "
        "ClickHouse >= 24.4); aborting startup fail-closed"
    )
