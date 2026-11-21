"""AWS: the client settings every context shares, and the port that serves their clients.

The SDK client is built by :mod:`loom.connections.aws`; importing this package loads no SDK.
"""

from __future__ import annotations

from loom.core.connections.aws.config import (
    AwsClientConfig,
    AwsPool,
    RetryMode,
    aws_error_code,
)
from loom.core.connections.aws.inheritance import AwsInheritance
from loom.core.connections.aws.source import AwsClientSource

__all__ = [
    "AwsClientConfig",
    "AwsClientSource",
    "AwsInheritance",
    "AwsPool",
    "RetryMode",
    "aws_error_code",
]
