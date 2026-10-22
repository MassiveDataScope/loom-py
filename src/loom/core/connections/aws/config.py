"""AWS client settings shared by every loom context that calls an AWS service.

The DynamoDB repository (``persistence.dynamodb``), the ETL DynamoDB sources
(``storage.dynamodb``), the ``secrets`` and ``ssm`` resolvers and the SNS
notifier build their clients from :class:`AwsClientConfig` through the one
builder of :mod:`loom.core.connections.aws.client`.

Every bound is finite by default: botocore waits 60 s for a read, longer than
a typical serverless invocation, so a hung call would consume the whole
invocation instead of failing with an error.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, ClassVar, Literal

import msgspec

from loom.core.connections._base import ConnectionPool, ConnectionSettings, omit_none
from loom.core.model.types import PositiveInt

RetryMode = Literal["standard", "adaptive"]
"""botocore retry strategies loom accepts; ``legacy`` retries five times by default."""


class AwsPool(ConnectionPool, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """``pool`` of an AWS client: the HTTP connections it keeps.

    Args:
        size: HTTP connections the client keeps; a repository runs that many
            calls in parallel.
    """

    size: PositiveInt = 32


class AwsClientConfig(ConnectionSettings, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Settings of one AWS SDK client.

    Credentials are never read from configuration: boto3's default credential
    chain applies (the task or function role, or the environment), narrowed to
    a named profile when *profile* is set.

    Args:
        region: AWS region; ``None`` lets boto3 resolve its own.
        url: Endpoint override for a local or fake endpoint; ``None`` is the
            AWS regional endpoint.
        profile: Named AWS profile; ``None`` uses the default chain.
        pool: HTTP connections the client keeps.
        connect_timeout: Seconds to establish a connection.
        read_timeout: Seconds to wait for a response.
        max_attempts: Total attempts per call, the first one included.
        retry_mode: botocore retry strategy.
    """

    _RETIRED_KEYS: ClassVar[Mapping[str, str]] = MappingProxyType(
        {"endpoint_url": "url", "max_pool_connections": "pool.size"}
    )

    region: str | None = None
    url: str | None = None
    profile: str | None = None
    pool: AwsPool = msgspec.field(default_factory=AwsPool)
    connect_timeout: PositiveInt = 2
    read_timeout: PositiveInt = 5
    max_attempts: PositiveInt = 3
    retry_mode: RetryMode = "standard"

    def client_options(self) -> dict[str, Any]:
        """Return the ``Session.client`` options, ``config`` as ``botocore.Config`` keywords.

        The island turns ``config`` into a ``botocore.config.Config``; an unset
        ``url`` is left out, so boto3 resolves the regional endpoint.
        """
        return {
            "config": {
                "max_pool_connections": self.pool.size,
                "connect_timeout": self.connect_timeout,
                "read_timeout": self.read_timeout,
                # total_max_attempts counts the first request; Config's own
                # max_attempts would count retries only.
                "retries": {"total_max_attempts": self.max_attempts, "mode": self.retry_mode},
            },
            **omit_none({"endpoint_url": self.url}),
        }


__all__ = ["AwsClientConfig", "AwsPool", "RetryMode"]
