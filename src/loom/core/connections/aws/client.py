"""The AWS SDK island: the one module of loom that builds a boto3 client.

Loaded with :func:`~loom.core.connections.aws.loader.aws_island` by every
context calling AWS, each naming the extra that brings boto3 to it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]

from loom.core.connections import merge_extra
from loom.core.connections.aws.config import AwsClientConfig

if TYPE_CHECKING:
    from loom.core.connections.aws import client as _self
    from loom.core.connections.aws.loader import AwsIsland

    _conforms: AwsIsland = _self

__all__ = ["build_aws_client"]


def build_aws_client(service: str, config: AwsClientConfig, **extra: Any) -> Any:
    """Build one low-level boto3 client for *service* from *config*.

    A session of its own backs each client: boto3's shared default session is
    not safe to use concurrently, a session must not be shared across threads,
    and a client, once built, is (boto3 "Clients" and "Session" guides). Build
    the client once and reuse it.

    Args:
        service: AWS service name, e.g. ``"dynamodb"`` or ``"ssm"``.
        config: Region, endpoint, profile and bounds of the client.
        **extra: Further ``Session.client`` options of the caller's context.

    Returns:
        The client; typed ``Any`` because boto3 ships no stubs.

    Raises:
        ValueError: When *extra* names an option *config* decides.
    """
    options = config.client_options()
    built = {**options, "config": Config(**options["config"])}
    session_options = {"profile_name": config.profile, "region_name": config.region}
    merged = merge_extra(built, extra, config, reserved=tuple(session_options))
    session = boto3.Session(**session_options)
    return session.client(service, **merged)
