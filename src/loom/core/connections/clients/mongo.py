"""The pymongo island: the one module of loom that builds a MongoDB client.

Loaded with ``import_optional("loom.core.connections.clients.mongo", extra="mongo")``.
Options passed as keywords win over those of the URI, as in the driver.
"""

from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient, MongoClient
from pymongo.server_api import ServerApi

from loom.core.connections._base import merge_extra
from loom.core.connections.mongo import MongoConnection

_RESERVED = ("host", "maxPoolSize", "serverSelectionTimeoutMS", "server_api")

__all__ = ["build_async_mongo_client", "build_mongo_client"]


def build_mongo_client(connection: MongoConnection, **extra: Any) -> MongoClient[Any]:
    """Build a synchronous client for *connection*.

    Args:
        connection: Address and client options.
        **extra: Options of the caller's context, e.g. ``datetime_conversion``.

    Returns:
        The client; it connects lazily.

    Raises:
        ValueError: When *extra* names an option *connection* decides.
    """
    return MongoClient(connection.uri, **_options(connection, extra))


def build_async_mongo_client(connection: MongoConnection, **extra: Any) -> AsyncMongoClient[Any]:
    """Build an asynchronous client for *connection*.

    Args:
        connection: Address and client options.
        **extra: Options of the caller's context, e.g. ``tz_aware``.

    Returns:
        The client; it connects lazily.

    Raises:
        ValueError: When *extra* names an option *connection* decides.
    """
    return AsyncMongoClient(connection.uri, **_options(connection, extra))


def _options(connection: MongoConnection, extra: dict[str, Any]) -> dict[str, Any]:
    built: dict[str, Any] = {}
    if connection.max_pool_size is not None:
        built["maxPoolSize"] = connection.max_pool_size
    if connection.server_selection_timeout_ms is not None:
        built["serverSelectionTimeoutMS"] = connection.server_selection_timeout_ms
    if connection.server_api_version is not None:
        built["server_api"] = ServerApi(connection.server_api_version)
    return merge_extra(built, extra, connection, reserved=_RESERVED)
