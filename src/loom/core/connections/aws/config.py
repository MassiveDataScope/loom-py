"""AWS client settings shared by every loom context that calls an AWS service.

The DynamoDB repository (a ``dynamodb`` store), the ETL DynamoDB sources
(``storage.dynamodb``), the ``secrets`` and ``ssm`` resolvers and the SNS
notifier build their clients from :class:`AwsClientConfig` through the one
builder of :mod:`loom.core.connections.aws.client`.

Every bound is finite by default: botocore waits 60 s for a read, longer than
a typical serverless invocation, so a hung call would consume the whole
invocation instead of failing with an error.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

import msgspec

from loom.core.connections._base import ConnectionPool, ConnectionSettings, omit_none
from loom.core.connections.aws.loader import aws_island
from loom.core.model.redaction import Dsn
from loom.core.model.types import PositiveInt

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

RetryMode = Literal["standard", "adaptive"]
"""botocore retry strategies loom accepts; ``legacy`` retries five times by default."""


class AwsService(StrEnum):
    """The AWS services loom builds a client for; the value is the boto3 service name."""

    DYNAMODB = "dynamodb"
    SQS = "sqs"
    SNS = "sns"
    SSM = "ssm"
    SECRETS_MANAGER = "secretsmanager"

    @property
    def extra(self) -> str:
        """The loom extra that brings boto3 to a caller of this service."""
        match self:
            case AwsService.DYNAMODB | AwsService.SQS:
                return self.value
            case AwsService.SNS:
                return "prefect"
            case AwsService.SSM | AwsService.SECRETS_MANAGER:
                return "config-ssm"


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
    url: Dsn | None = None
    profile: str | None = None
    pool: AwsPool = msgspec.field(default_factory=AwsPool)
    connect_timeout: PositiveInt = 2
    read_timeout: PositiveInt = 5
    max_attempts: PositiveInt = 3
    retry_mode: RetryMode = "standard"

    @classmethod
    def inherit(cls, ctx: ConfigContext, own: Mapping[str, Any], path: str) -> dict[str, Any]:
        """Return *own* with the keys it does not set taken from the ``aws:`` section of *ctx*.

        Args:
            ctx: Configuration holding the optional top-level ``aws:`` section.
            own: The product section, overriding ``aws:`` key by key.
            path: Dotted path of *own*, for the messages.

        Returns:
            The merged mapping.

        Raises:
            ConfigError: When ``aws:`` or *own* holds a retired key, or ``aws:`` holds a key
                no client takes.
        """
        from loom.core.config.errors import ConfigError
        from loom.core.config.keys import ConfigKey
        from loom.core.config.retired import reject_retired_keys

        aws = ctx.section(ConfigKey.AWS, dict) if ctx.has(ConfigKey.AWS) else {}
        reject_retired_keys(aws, ConfigKey.AWS, AwsClientConfig)
        reject_retired_keys(own, path, cls)
        unknown = sorted(set(aws) - set(AwsClientConfig.__struct_encode_fields__))
        if unknown:
            raise ConfigError(f"Config section {ConfigKey.AWS.value!r} takes no key {unknown[0]!r}")
        return {**aws, **own}

    @classmethod
    def from_config(
        cls,
        ctx: ConfigContext,
        path: str,
        *,
        own: Mapping[str, Any] | None = None,
        **over: Any,
    ) -> Self:
        """Decode the settings of the product section at *path*, inheriting ``aws:``.

        Args:
            ctx: Configuration holding the optional top-level ``aws:`` section.
            path: Dotted path of the product section, for the messages.
            own: The product section; ``None`` is an empty one.
            **over: Settings that win over both sections.

        Returns:
            The decoded settings.

        Raises:
            ConfigError: When a section holds a retired or unknown key, or a value is invalid.
        """
        from loom.core.config.loader import convert_section

        return convert_section(cls.inherit(ctx, own or {}, path) | over, path, cls)

    def build_client(self, service: AwsService, **options: Any) -> Any:
        """Build one boto3 client for *service* from these settings.

        Args:
            service: The AWS service the client talks to.
            **options: Further ``Session.client`` options of the caller's context.

        Returns:
            The client; typed ``Any`` because boto3 ships no stubs.

        Raises:
            MissingExtraError: When boto3 is not installed, naming the extra of *service*.
            ValueError: When *options* names an option these settings decide.
        """
        return aws_island(extra=service.extra).build_aws_client(service, self, **options)

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
                "retries": {"total_max_attempts": self.max_attempts, "mode": self.retry_mode},
            },
            **omit_none({"endpoint_url": self.url}),
        }


def aws_error_code(exc: BaseException) -> str:
    """Return the AWS error code of *exc*, or its exception type name.

    ``botocore.exceptions.ClientError`` is raised for nearly every AWS API
    failure, so its type name alone cannot tell a missing parameter from an
    access denial, a disabled KMS key, or throttling. The ``Error.Code``
    field of the response carries that distinction. It is an enum-like
    constant chosen by AWS, so it can never echo the expanded path.

    Args:
        exc: Exception raised by the AWS client.

    Returns:
        The AWS error code when the exception carries a botocore-shaped
        ``response`` mapping, otherwise ``type(exc).__name__``.
    """
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error")
        if isinstance(error, dict):
            code = error.get("Code")
            if isinstance(code, str) and code:
                return code
    return type(exc).__name__


__all__ = ["AwsClientConfig", "AwsPool", "AwsService", "RetryMode", "aws_error_code"]
