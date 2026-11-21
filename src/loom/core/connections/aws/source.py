"""The port through which a context asks for the AWS client of a service."""

from __future__ import annotations

from typing import Any, Protocol

from loom.core.connections.aws.config import AwsClientConfig


class AwsClientSource(Protocol):
    """Serves one AWS client per service and distinct client settings."""

    def client(self, service: str, config: AwsClientConfig) -> Any:
        """Return the client of *service* for *config*; ``Any`` because boto3 ships no stubs."""
        ...

    def close(self) -> None:
        """Release the connections of every client served so far."""
        ...
