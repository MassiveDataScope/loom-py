"""Driver boundary for the ClickHouse SQL backend.

Centralizes every touch point with ``clickhouse-connect``: the guarded import
with the install hint, the structural client contract shared by the executor
and the registry, the adapter over the driver ``AsyncClient``, the multi-role
transport workaround and the sanitization of driver error messages (no host,
DSN or stack trace).
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from collections.abc import Set as AbstractSet
from functools import partial
from typing import Any, Protocol, cast
from urllib.parse import urlencode

_INSTALL_HINT = (
    "The ClickHouse SQL backend requires 'clickhouse-connect'. "
    "Install it with: pip install 'loom-kernel[clickhouse]'"
)

try:
    from clickhouse_connect.driver import httpclient  # type: ignore[import-untyped]
    from clickhouse_connect.driver.exceptions import (  # type: ignore[import-untyped]
        DatabaseError,
        OperationalError,
    )

    from loom.core.connections.clickhouse.client import open_async_client
except ImportError as exc:  # pragma: no cover - only without the optional extra
    raise ImportError(_INSTALL_HINT) from exc

__all__ = [
    "AsyncClickHouseClient",
    "ClickHouseClientFactory",
    "ClickHouseColumnType",
    "ClickHouseQueryResult",
    "DatabaseError",
    "OperationalError",
    "create_driver_client",
    "enable_repeated_query_params",
    "sanitize_backend_error",
    "supports_repeated_query_params",
]

_CODE_LINE_RE = re.compile(r"Code:\s*\d+[^\n]*")
_URL_RE = re.compile(r"https?://\S+")

# --- Multi-role transport workaround (pending upstream) ----------------------
#
# ClickHouse activates several roles only through REPEATED HTTP parameters
# ("role=a&role=b"); a comma-joined value is parsed as one role name and
# rejected with code 511. clickhouse-connect 0.15.1 builds the request URL in
# ``HttpClient._raw_request`` with ``urlencode(final_params)`` — without
# ``doseq=True`` — so a sequence value would be sent as its repr
# ("role=%5B%27a%27%2C+%27b%27%5D"), an invalid role name. Upstream ``main``
# still encodes it the same way, so this is not fixed by a version bump.
#
# Rebinding the encoder the driver looks up is the smallest correct seam:
# ``doseq=True`` produces byte-identical output for the ``str``/``int``/``bool``
# values the driver builds today and only changes sequences, whose current
# encoding is invalid anyway. Overriding ``_raw_request`` instead would mean
# duplicating ~80 lines of driver internals (retry loop, session guard, error
# handling) with no way to reuse the URL building alone.
#
# The rebinding is NOT applied at import: mutating a third-party namespace for
# the whole process — including consumers that never touch Loom — as a side
# effect of an import is exactly the hidden global state this framework
# forbids. The registry enables it explicitly, and only for a connection that
# can ever apply more than one role.
#
# ``supports_repeated_query_params`` re-checks the outcome so the registry can
# fail closed on a driver that stops exposing the seam, and
# ``tests/unit/core/sql/test_clickhouse_transport.py`` pins the patch point so
# the workaround can never degrade silently.
_MULTI_ROLE_PROBE: Mapping[str, Any] = {"role": ("loom_probe_a", "loom_probe_b")}
_MULTI_ROLE_PROBE_ENCODED = "role=loom_probe_a&role=loom_probe_b"
_URLENCODE_ATTR = "urlencode"


def enable_repeated_query_params() -> None:
    """Make the driver emit one HTTP parameter per sequence-valued setting.

    Idempotent and explicit: rebinds the ``urlencode`` the driver looks up when
    building a request URL, so a tuple of roles travels as ``role=a&role=b``
    instead of its ``repr``.  Does nothing when the driver no longer exposes
    that symbol — callers must then check
    :func:`supports_repeated_query_params` and fail closed.
    """
    if getattr(httpclient, _URLENCODE_ATTR, None) is None:
        return
    httpclient.urlencode = partial(urlencode, doseq=True)


def supports_repeated_query_params() -> bool:
    """Report whether the driver emits one HTTP parameter per sequence value.

    Checks that the driver still exposes the encoder seam *and* that it now
    produces repeated parameters, so a future driver version that drops the
    symbol is detected instead of silently sending an invalid single role.

    Returns:
        ``True`` when a sequence-valued setting is serialized as repeated
        parameters, which is what multi-role queries require.
    """
    encoder = getattr(httpclient, _URLENCODE_ATTR, None)
    if encoder is None:
        return False
    encoded: str = encoder(_MULTI_ROLE_PROBE)
    return encoded == _MULTI_ROLE_PROBE_ENCODED


class ClickHouseColumnType(Protocol):
    """Driver column type exposing its native ClickHouse name."""

    @property
    def name(self) -> str:
        """Native type name (e.g. ``"DateTime64(3, 'UTC')"``)."""
        ...


class ClickHouseQueryResult(Protocol):
    """Surface of the driver query result consumed by the executor."""

    @property
    def result_rows(self) -> Sequence[Sequence[Any]]:
        """Materialized result rows."""
        ...

    @property
    def column_names(self) -> Sequence[str]:
        """Column names in result order."""
        ...

    @property
    def column_types(self) -> Sequence[ClickHouseColumnType]:
        """Column types in result order."""
        ...


class AsyncClickHouseClient(Protocol):
    """Async client surface required by the executor and the registry."""

    @property
    def valid_transport_settings(self) -> AbstractSet[str]:
        """Per-query settings the driver accepts as HTTP transport settings."""
        ...

    async def query(
        self,
        query: str,
        *,
        parameters: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
    ) -> ClickHouseQueryResult:
        """Run *query* with server-side *parameters* and per-query *settings*."""
        ...

    async def close(self) -> None:
        """Release the underlying connection resources."""
        ...


class ClickHouseClientFactory(Protocol):
    """Factory invoked with the exact ``get_async_client`` keyword arguments."""

    async def __call__(self, **kwargs: Any) -> AsyncClickHouseClient:
        """Create a client for one connection from driver keyword arguments."""
        ...


class _DriverAsyncClientAdapter:
    """Adapts the driver ``AsyncClient`` to :class:`AsyncClickHouseClient`.

    The driver exposes ``valid_transport_settings`` only on the wrapped sync
    client; the adapter surfaces it next to the async ``query``/``close``.
    """

    def __init__(self, async_client: Any) -> None:
        self._async_client = async_client

    @property
    def valid_transport_settings(self) -> AbstractSet[str]:
        """Per-query transport settings reported by the wrapped sync client."""
        return frozenset(self._async_client.client.valid_transport_settings)

    async def query(
        self,
        query: str,
        *,
        parameters: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
    ) -> ClickHouseQueryResult:
        """Delegate to the driver client (untyped boundary, hence the cast)."""
        result = await self._async_client.query(
            query=query, parameters=parameters, settings=settings
        )
        return cast(ClickHouseQueryResult, result)

    async def close(self) -> None:
        """Close the driver client and shut down its thread pool."""
        await self._async_client.close()


async def create_driver_client(**kwargs: Any) -> AsyncClickHouseClient:
    """Create an adapted driver ``AsyncClient`` from ``get_async_client`` kwargs.

    The client is opened by the ClickHouse island,
    :mod:`loom.core.connections.clickhouse.client`, the one place loom builds
    a ClickHouse client.

    Args:
        **kwargs: Keyword arguments forwarded verbatim to
            ``clickhouse_connect.get_async_client``.

    Returns:
        The created client adapted to :class:`AsyncClickHouseClient`.
    """
    return _DriverAsyncClientAdapter(await open_async_client(kwargs))


def sanitize_backend_error(message: str) -> str:
    """Reduce a driver error message to a single sanitized line.

    Keeps the ``Code: NNN`` line when present (first line otherwise) and
    redacts any URL so host or DSN details never leak to callers.

    Args:
        message: Raw driver exception message.

    Returns:
        One line carrying the ClickHouse error code, safe to expose.
    """
    match = _CODE_LINE_RE.search(message)
    if match is not None:
        line = match.group(0)
    else:
        lines = message.splitlines()
        line = lines[0] if lines else "unknown backend error"
    return _URL_RE.sub("<redacted>", line).strip()
