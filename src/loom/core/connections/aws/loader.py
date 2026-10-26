"""The typed way to load the AWS island, and what the island publishes.

Importing this module loads no SDK: :func:`aws_island` loads
:mod:`loom.core.connections.aws.client` through
:func:`~loom.core.plugins.optional.import_optional`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, cast

from loom.core.plugins.optional import import_optional

if TYPE_CHECKING:
    from loom.core.connections.aws.config import AwsClientConfig

_ISLAND = "loom.core.connections.aws.client"


class AwsIsland(Protocol):
    """What :mod:`loom.core.connections.aws.client` publishes."""

    def build_aws_client(self, service: str, config: AwsClientConfig, **extra: Any) -> Any: ...


def aws_island(*, extra: str) -> AwsIsland:
    """Load the AWS island; *extra* is the loom extra that brings boto3 to the caller.

    Raises:
        MissingExtraError: When boto3 is not installed.
    """
    return cast(AwsIsland, import_optional(_ISLAND, extra=extra))


__all__ = ["AwsIsland", "aws_island"]
