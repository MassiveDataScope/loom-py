"""The AWS SDK island: the one module of loom that builds a boto3 client.

Loaded with ``import_optional("loom.core.connections.clients.aws", extra=...)``
by every context calling AWS, each naming the extra that brings boto3 to it.
"""

from __future__ import annotations

from typing import Any

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]

from loom.core.connections._base import merge_extra
from loom.core.connections.aws import AwsClientConfig

_RESERVED = ("endpoint_url", "config", "region_name", "service_name")

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
    options = merge_extra(
        {
            "endpoint_url": config.endpoint_url,
            "config": Config(
                max_pool_connections=config.max_pool_connections,
                connect_timeout=config.connect_timeout,
                read_timeout=config.read_timeout,
                # total_max_attempts counts the first request; Config's own
                # max_attempts would count retries only.
                retries={"total_max_attempts": config.max_attempts, "mode": config.retry_mode},
            ),
        },
        extra,
        config,
        reserved=_RESERVED,
    )
    session = boto3.Session(profile_name=config.profile, region_name=config.region)
    return session.client(service, **options)
