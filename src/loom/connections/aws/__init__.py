"""AWS: the clients of one process, the loader of the SDK that builds them, and the resolvers.

Importing this package loads no SDK; the SDK is touched only by
:mod:`loom.connections.aws.client`, loaded with :func:`aws_island` by :class:`AwsClients`.
:class:`SsmResolver` and :class:`SecretsManagerResolver` resolve the ``${ssm:...}`` and
``${secrets:...}`` placeholders of a configuration through those clients.
"""

from __future__ import annotations

from loom.connections.aws.clients import AwsClients
from loom.connections.aws.loader import AwsIsland, aws_island
from loom.connections.aws.secrets import SecretsManagerResolver
from loom.connections.aws.ssm import SsmResolver

__all__ = [
    "AwsClients",
    "AwsIsland",
    "SecretsManagerResolver",
    "SsmResolver",
    "aws_island",
]
