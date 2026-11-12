"""What a Lambda entry point refuses to compose, so nothing it cannot serve disappears silently."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING

from loom.core.config import ConfigKey
from loom.core.config.errors import ConfigError
from loom.core.logger import get_logger

if TYPE_CHECKING:
    from loom.ai.config import AiConfig
    from loom.composition import CompositionSource

COMPONENT = "LambdaAdapter"
"""The adapter refusals and composition errors name."""
_STDIO = "stdio"
"""The MCP transport that runs a server as a subprocess, which a frozen function cannot keep."""

_log = get_logger(__name__)


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
    interpolation is not seen here: :func:`check_resolved_ai` refuses it at Init.

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
    _refuse_stdio(_written_transports(source.config.raw(f"{ConfigKey.AI}.mcp_servers")))


def check_resolved_ai(config: AiConfig | None) -> None:
    """Refuse and warn, once at Init, on the ``ai`` section as resolved.

    Catches a stdio transport given by an interpolation, which
    :func:`refuse_unserved_config` cannot see, and logs one warning per agent
    endpoint key, ignored because a Lambda function serves no agent endpoint.

    Args:
        config: The decoded ``ai`` section, or ``None`` when it is absent.

    Raises:
        LambdaUnservedConfigError: When an MCP server resolves to ``transport: stdio``.
    """
    if config is None:
        return
    _refuse_stdio({name: server.transport for name, server in config.mcp_servers.items()})
    for key in _endpoint_keys(config):
        _log.warning(f"{key} is ignored on Lambda: {COMPONENT} serves no agent endpoint", key=key)


def _written_transports(servers: object) -> dict[str, object]:
    """The transport of each server of ``ai.mcp_servers`` as written, by name."""
    if not isinstance(servers, dict):
        return {}
    return {
        name: server.get("transport")
        for name, server in servers.items()
        if isinstance(server, dict)
    }


def _refuse_stdio(transports: Mapping[str, object]) -> None:
    """Refuse the first server of *transports*, by name, spoken to over stdio."""
    for name, transport in transports.items():
        if transport == _STDIO:
            raise LambdaUnservedConfigError(
                f"{ConfigKey.AI}.mcp_servers.{name}.transport",
                f"{COMPONENT} cannot run an MCP server over stdio: a frozen function keeps "
                "no subprocess. Reach the server over http, or serve it with FastAPIAdapter.",
            )


def _endpoint_keys(config: AiConfig) -> Iterator[str]:
    """The keys of *config* that would serve an agent over HTTP or A2A."""
    for name, endpoint in config.endpoints.items():
        if endpoint.enabled:
            yield f"{ConfigKey.AI}.endpoints.{name}.enabled"
    if config.a2a is not None:
        yield f"{ConfigKey.AI}.a2a.expose"
