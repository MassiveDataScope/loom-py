"""Common contracts for component discovery engines."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any, Protocol

from loom.core.job.job import Job
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
        jobs: Discovered jobs, whose ``jobs.<Job>`` overrides the composition
            applies; empty for engines that do not expose jobs.
    """

    models: tuple[type[BaseModel], ...]
    use_cases: tuple[type[UseCase[object, object]], ...]
    interfaces: tuple[type[RestInterface[object]], ...]
    agent_specs: tuple[str, ...] = ()
    jobs: tuple[type[Job[Any]], ...] = ()

    def with_changes(self, **changes: Any) -> DiscoveryResult:
        """Return a copy with *changes* applied, keeping every other field (later ones too)."""
        current = {field.name: getattr(self, field.name) for field in fields(self)}
        return DiscoveryResult(**(current | changes))


class DiscoveryEngine(Protocol):
    """Discovery strategy contract."""

    def discover(self) -> DiscoveryResult:
        """Return discovered models, use cases, and REST interfaces."""
        ...
