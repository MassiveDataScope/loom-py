"""ClickHouse connection settings shared by the SQL service, the ETL and streaming sinks.

The SQL service (``sql.connections.<name>``), the ETL (``storage.clickhouse``)
and the streaming sinks (``database.<name>`` named by a sink) declare
:class:`ClickHouseConnection` or a subclass of it, and build their clients
through :mod:`loom.core.connections.clients.clickhouse`.
"""

from __future__ import annotations

from typing import ClassVar

from loom.core.connections._base import ConnectionSettings, require_text
from loom.core.model.types import PositiveInt


class ClickHouseConnection(
    ConnectionSettings, frozen=True, kw_only=True, forbid_unknown_fields=True
):
    """Address, credentials and transport bounds of one ClickHouse server.

    Explicit credentials win over those of the DSN, as in the driver. A bound
    left ``None`` keeps the driver default (10 s to connect, 300 s to send or
    receive): a context with a bound of its own redeclares the field.

    Args:
        url: ClickHouse DSN, e.g. ``clickhouse://host:8123/db``.
        username: Driver username, handed over as an argument.
        password: Driver password, handed over as an argument: never
            URL-parsed, so any character is safe, and never shown in a repr.
        connect_timeout: Seconds to connect, above zero.
        send_receive_timeout: Seconds to send or receive, above zero.
        executor_threads: Thread pool of the asynchronous client.
        pool_size: HTTP connections the client keeps.
    """

    _SECRET_FIELDS: ClassVar[frozenset[str]] = frozenset({"password"})
    _URI_FIELDS: ClassVar[frozenset[str]] = frozenset({"url"})

    url: str
    username: str | None = None
    password: str | None = None
    connect_timeout: PositiveInt | None = None
    send_receive_timeout: PositiveInt | None = None
    executor_threads: PositiveInt | None = None
    pool_size: PositiveInt | None = None

    def __post_init__(self) -> None:
        require_text(self.url, "url")


__all__ = ["ClickHouseConnection"]
