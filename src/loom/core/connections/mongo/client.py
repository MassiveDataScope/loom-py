"""The pymongo island: the one module of loom that builds a MongoDB client.

Loaded with :func:`~loom.core.connections.mongo.loader.mongo_island`. Options
passed as keywords win over those of the URL, as in the driver.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pymongo import AsyncMongoClient, MongoClient
from pymongo.server_api import ServerApi

from loom.core.connections._base import merge_extra
from loom.core.connections.mongo.config import MongoConnection

if TYPE_CHECKING:
    from loom.core.connections.mongo import client as _self
    from loom.core.connections.mongo.loader import MongoIsland

    _conforms: MongoIsland = _self

__all__ = ["build_async_mongo_client", "build_mongo_client"]


def build_mongo_client(connection: MongoConnection, **extra: Any) -> MongoClient[Any]:
    """Build a synchronous client for *connection*.

    Args:
        connection: Address, credentials and bounds.
        **extra: Options of the caller's context, e.g. ``datetime_conversion``.

    Returns:
        The client; it connects lazily.

    Raises:
        ValueError: When *extra* names an option *connection* decides.
    """
    return MongoClient(**_options(connection, extra))


def build_async_mongo_client(connection: MongoConnection, **extra: Any) -> AsyncMongoClient[Any]:
    """Build an asynchronous client for *connection*.

    Args:
        connection: Address, credentials and bounds.
        **extra: Options of the caller's context, e.g. ``tz_aware``.

    Returns:
        The client; it connects lazily.

    Raises:
        ValueError: When *extra* names an option *connection* decides.
    """
    return AsyncMongoClient(**_options(connection, extra))


def _options(connection: MongoConnection, extra: dict[str, Any]) -> dict[str, Any]:
    built = connection.client_options()
    if connection.server_api_version is not None:
        built["server_api"] = ServerApi(connection.server_api_version)
    return merge_extra(built, extra, connection)
