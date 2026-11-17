"""The AWS SDK island: the one module of loom that touches boto3.

Loaded with :func:`~loom.core.connections.aws.loader.aws_island` by
:class:`~loom.core.connections.aws.clients.AwsClients`; the ``aws`` extra brings boto3 to it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]

from loom.core.connections.aws.config import AwsClientConfig
from loom.core.model.redaction import reveal_for_driver

if TYPE_CHECKING:
    from loom.core.connections.aws import client as _self
    from loom.core.connections.aws.loader import AwsIsland

    _conforms: AwsIsland = _self

__all__ = ["build_aws_client", "open_session"]


def open_session(profile: str | None) -> Any:
    """Open one boto3 session.

    boto3's shared default session is not safe to use concurrently and a
    session must not be shared across threads while clients are built from it
    (boto3 "Session" guide): the caller builds every client of the session
    under one lock.

    Args:
        profile: Named AWS profile; ``None`` uses the default credential chain.

    Returns:
        The session; typed ``Any`` because boto3 ships no stubs.
    """
    return boto3.Session(profile_name=profile)


def build_aws_client(session: Any, service: str, config: AwsClientConfig) -> Any:
    """Build one low-level boto3 client for *service* on *session* from *config*.

    A client, once built, is safe to share across threads (boto3 "Clients"
    guide): build it once and reuse it.

    Args:
        session: A session of :func:`open_session`.
        service: AWS service name, e.g. ``"dynamodb"`` or ``"ssm"``.
        config: Region, endpoint and bounds of the client.

    Returns:
        The client; typed ``Any`` because boto3 ships no stubs.
    """
    options = reveal_for_driver(config.client_options())
    botocore = Config(**options["config"])
    return session.client(service, **{**options, "config": botocore}, region_name=config.region)
