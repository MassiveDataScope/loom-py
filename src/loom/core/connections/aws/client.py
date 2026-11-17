"""The AWS SDK island: the one module of loom that touches boto3.

Loaded with :func:`~loom.core.connections.aws.loader.aws_island` by
:class:`~loom.core.connections.aws.clients.AwsClients`; the ``aws`` extra brings boto3 to it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]
from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]

from loom.core.connections.aws.config import AwsClientConfig
from loom.core.model.redaction import reveal_for_driver

if TYPE_CHECKING:
    from loom.core.connections.aws import client as _self
    from loom.core.connections.aws.loader import AwsIsland

    _conforms: AwsIsland = _self

__all__ = ["build_aws_client", "open_session", "sdk_errors"]


def open_session(profile: str | None) -> Any:
    """Open one boto3 session.

    Args:
        profile: Named AWS profile; ``None`` uses the default credential chain.

    Returns:
        The session; typed ``Any`` because boto3 ships no stubs.
    """
    return boto3.Session(profile_name=profile)


def build_aws_client(session: Any, service: str, config: AwsClientConfig) -> Any:
    """Build one low-level boto3 client for *service* on *session* from *config*.

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


def sdk_errors() -> tuple[type[Exception], ...]:
    """The errors the SDK raises for a failed call: its own, and the service's answer."""
    return (BotoCoreError, ClientError)
