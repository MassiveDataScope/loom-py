"""AWS: the clients of one process, and the loader of the SDK that builds them.

Importing this package loads no SDK; the SDK is touched only by
:mod:`loom.connections.aws.client`, loaded with :func:`aws_island` by :class:`AwsClients`.
"""

from __future__ import annotations

from loom.connections.aws.clients import AwsClients
from loom.connections.aws.loader import AwsIsland, aws_island

__all__ = ["AwsClients", "AwsIsland", "aws_island"]
