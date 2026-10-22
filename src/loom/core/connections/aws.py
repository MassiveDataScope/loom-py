"""AWS client settings shared by every loom context that calls an AWS service.

The DynamoDB repository (``persistence.dynamodb``), the ETL DynamoDB sources
(``storage.dynamodb``), the ``secrets`` and ``ssm`` resolvers and the SNS
notifier build their clients from :class:`AwsClientConfig` through the one
builder of :mod:`loom.core.connections.clients.aws`.

Every bound is finite by default: botocore waits 60 s for a read, longer than
a typical serverless invocation, so a hung call would consume the whole
invocation instead of failing with an error.
"""

from __future__ import annotations

from typing import Literal

from loom.core.connections._base import ConnectionSettings
from loom.core.model.types import PositiveInt

RetryMode = Literal["standard", "adaptive"]
"""botocore retry strategies loom accepts; ``legacy`` retries five times by default."""


class AwsClientConfig(ConnectionSettings, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Settings of one AWS SDK client.

    Credentials are never read from configuration: boto3's default credential
    chain applies (the task or function role, or the environment), narrowed to
    a named profile when *profile* is set.

    Args:
        region: AWS region; ``None`` lets boto3 resolve its own.
        endpoint_url: Override for a local or fake endpoint.
        profile: Named AWS profile; ``None`` uses the default chain.
        max_pool_connections: HTTP connections the client keeps; a repository
            runs that many calls in parallel.
        connect_timeout: Seconds to establish a connection.
        read_timeout: Seconds to wait for a response.
        max_attempts: Total attempts per call, the first one included.
        retry_mode: botocore retry strategy.
    """

    region: str | None = None
    endpoint_url: str | None = None
    profile: str | None = None
    max_pool_connections: PositiveInt = 32
    connect_timeout: PositiveInt = 2
    read_timeout: PositiveInt = 5
    max_attempts: PositiveInt = 3
    retry_mode: RetryMode = "standard"


__all__ = ["AwsClientConfig", "RetryMode"]
