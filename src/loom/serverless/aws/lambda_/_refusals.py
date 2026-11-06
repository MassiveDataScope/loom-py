"""What a Lambda entry point refuses to compose, so nothing it cannot serve disappears silently."""

from __future__ import annotations

from typing import TYPE_CHECKING

from loom.core.config import ConfigKey
from loom.core.config.errors import ConfigError

if TYPE_CHECKING:
    from loom.composition import CompositionSource

COMPONENT = "LambdaAdapter"
"""The adapter refusals and composition errors name."""
MANIFEST_AGENTS_KEY = "manifest.AGENTS"
"""The :attr:`LambdaUnservedConfigError.key` of the agents a manifest module declares."""


class LambdaUnservedConfigError(ConfigError):
    """Raised when the configuration declares something a Lambda adapter does not serve.

    Raised by the recipe, before anything opens.

    Attributes:
        key: Where the unserved declaration lives: a configuration key, or
            :data:`MANIFEST_AGENTS_KEY` for the agents a manifest module declares.
    """

    def __init__(self, key: str, message: str) -> None:
        super().__init__(f"{key}: {message}")
        self.key = key


def refuse_unserved_config(source: CompositionSource) -> None:
    """Refuse, from the configuration alone, what no Lambda function serves.

    Runs before discovery: a metrics scrape and an agent runtime.

    Args:
        source: What the entry point composes from.

    Raises:
        LambdaUnservedConfigError: When metrics are enabled or an ``ai``
            section is declared.
    """
    if source.observability_config.prometheus.enabled:
        raise LambdaUnservedConfigError(
            "observability.prometheus.enabled",
            "a Lambda function serves no metrics scrape. Disable it for this entry point.",
        )
    if source.config.has(ConfigKey.AI):
        raise LambdaUnservedConfigError(
            ConfigKey.AI,
            f"{COMPONENT} does not host agents. Serve them with FastAPIAdapter, "
            "or drop the section from this entry point's configuration.",
        )


def refuse_manifest_agents(agent_specs: tuple[str, ...]) -> None:
    """Refuse the agents a manifest module declares, once discovery has read them.

    Raises:
        LambdaUnservedConfigError: When *agent_specs* is not empty.
    """
    if agent_specs:
        raise LambdaUnservedConfigError(
            MANIFEST_AGENTS_KEY,
            f"the manifest declares agents, which {COMPONENT} does not host. "
            "Serve them with FastAPIAdapter.",
        )
