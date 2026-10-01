"""Common contracts for component discovery engines."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from loom.core.model import BaseModel
from loom.core.use_case.use_case import UseCase

if TYPE_CHECKING:
    from loom.rest.model import RestInterface

AGENTS_ONLY_HINT = (
    "An application whose only content is agents uses app.discovery.mode: manifest "
    "and declares AGENTS in the manifest module."
)
"""Hint appended to discovery/bootstrap errors an agents-only app can hit."""


@dataclass(frozen=True)
class DiscoveryResult:
    """Discovered application components used by bootstrap.

    Attributes:
        models: Discovered persistent models.
        use_cases: Discovered use cases, including auto-generated ones.
        interfaces: Discovered REST interfaces.
        agent_specs: Agent artifact paths or globs the manifest declares;
            empty for engines that do not expose agents.
    """

    models: tuple[type[BaseModel], ...]
    use_cases: tuple[type[UseCase[object, object]], ...]
    interfaces: tuple[type[RestInterface[object]], ...]
    agent_specs: tuple[str, ...] = ()


class DiscoveryEngine(Protocol):
    """Discovery strategy contract."""

    def discover(self) -> DiscoveryResult:
        """Return discovered models, use cases, and REST interfaces."""
        ...
