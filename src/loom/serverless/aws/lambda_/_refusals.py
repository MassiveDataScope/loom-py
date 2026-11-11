"""What a Lambda entry point refuses to compose, so nothing it cannot serve disappears silently."""

from __future__ import annotations

from typing import TYPE_CHECKING

from loom.core.config import ConfigKey
from loom.core.config.errors import ConfigError

if TYPE_CHECKING:
    from loom.composition import CompositionSource

COMPONENT = "LambdaAdapter"
"""The adapter refusals and composition errors name."""
_STDIO = "stdio"
"""The MCP transport that runs a server as a subprocess, which a frozen function cannot keep."""


class LambdaUnservedConfigError(ConfigError):
    """Raised when the configuration declares something a Lambda adapter does not serve.

    Raised by the recipe, before anything opens.

    Attributes:
        key: The configuration key of the unserved declaration.
    """

    def __init__(self, key: str, message: str) -> None:
        super().__init__(f"{key}: {message}")
        self.key = key


def refuse_unserved_config(source: CompositionSource) -> None:
    """Refuse, from the configuration alone, what no Lambda function serves.

    Runs before discovery: a metrics scrape and an MCP server spoken to over
    stdio. The ``ai`` section is read as written, so a transport given by an
    interpolation is not seen here.

    Args:
        source: What the entry point composes from.

    Raises:
        LambdaUnservedConfigError: When metrics are enabled or an MCP server
            declares ``transport: stdio``.
    """
    if source.observability_config.prometheus.enabled:
        raise LambdaUnservedConfigError(
            "observability.prometheus.enabled",
            "a Lambda function serves no metrics scrape. Disable it for this entry point.",
        )
    _refuse_stdio_servers(source.config.raw(f"{ConfigKey.AI}.mcp_servers"))


def _refuse_stdio_servers(servers: object) -> None:
    """Refuse the first of *servers*, ``ai.mcp_servers`` as written, spoken to over stdio."""
    if not isinstance(servers, dict):
        return
    for name, server in servers.items():
        if isinstance(server, dict) and server.get("transport") == _STDIO:
            raise LambdaUnservedConfigError(
                f"{ConfigKey.AI}.mcp_servers.{name}.transport",
                f"{COMPONENT} cannot run an MCP server over stdio: a frozen function keeps "
                "no subprocess. Reach the server over http, or serve it with FastAPIAdapter.",
            )
