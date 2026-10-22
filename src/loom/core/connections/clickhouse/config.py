"""ClickHouse connection settings shared by the SQL service, the ETL and streaming sinks.

The SQL service (``sql.connections.<name>``), the ETL (``storage.clickhouse``)
and the streaming sinks (``database.<name>`` named by a sink) declare
:class:`ClickHouseConnection` or a subclass of it, and build their clients
through :mod:`loom.core.connections.clickhouse.client`.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, ClassVar

import msgspec

from loom.core.connections._base import ConnectionPool, ConnectionSettings, require_text
from loom.core.model.types import PositiveInt


class ClickHouseConnection(
    ConnectionSettings, frozen=True, kw_only=True, forbid_unknown_fields=True
):
    """Address, credentials and transport bounds of one ClickHouse server.

    Explicit credentials win over those of the DSN, as in the driver. A bound
    left ``None`` keeps the driver default (10 s to connect, 300 s to read a
    response): a context with a bound of its own redeclares the field.

    Args:
        url: ClickHouse DSN, e.g. ``clickhouse://host:8123/db``.
        username: Driver username, handed over as an argument.
        password: Driver password, handed over as an argument: never
            URL-parsed, so any character is safe, and never shown in a repr.
        connect_timeout: Seconds to connect, above zero.
        read_timeout: Seconds to send a query or receive its response, above zero.
        executor_threads: Thread pool of the asynchronous client.
        pool: HTTP connections the client keeps.
    """

    _RETIRED_KEYS: ClassVar[Mapping[str, str]] = MappingProxyType(
        {"send_receive_timeout": "read_timeout", "pool_size": "pool.size"}
    )

    url: str
    username: str | None = None
    password: str | None = None
    connect_timeout: PositiveInt | None = None
    read_timeout: PositiveInt | None = None
    executor_threads: PositiveInt | None = None
    pool: ConnectionPool = msgspec.field(default_factory=ConnectionPool)

    def __post_init__(self) -> None:
        require_text(self.url, "url")

    def client_options(self) -> dict[str, Any]:
        """Return the ``get_client`` keyword options; ``read_timeout`` is ``send_receive_timeout``.

        The pool and the thread pool are driver objects or asynchronous-only
        options, left to the island.
        """
        named = {
            "username": self.username,
            "password": self.password,
            "connect_timeout": self.connect_timeout,
            "send_receive_timeout": self.read_timeout,
        }
        return {"dsn": self.url, **{k: v for k, v in named.items() if v is not None}}


__all__ = ["ClickHouseConnection"]
