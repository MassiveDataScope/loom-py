"""AWS: the client settings every context shares, and the typed loader of its island.

Importing this package loads no SDK; the client is built by
:mod:`loom.core.connections.aws.client`, loaded with :func:`aws_island`.
"""

from __future__ import annotations

from loom.core.connections.aws.config import AwsClientConfig, AwsPool, RetryMode
from loom.core.connections.aws.loader import AwsIsland, aws_island

__all__ = ["AwsClientConfig", "AwsIsland", "AwsPool", "RetryMode", "aws_island"]
