"""Common contracts for component discovery engines."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any

from loom.core.di.installer import Installer
from loom.core.job.job import Job
from loom.core.model import BaseModel
from loom.core.use_case.use_case import UseCase

if TYPE_CHECKING:
    from loom.rest.model import RestInterface


@dataclass(frozen=True)
class DiscoveryResult:
    """Discovered application components used by bootstrap.

    Attributes:
        models: Discovered persistent models.
        use_cases: Discovered use cases, including auto-generated ones.
        interfaces: Discovered REST interfaces.
        agent_specs: Agent artifact paths or globs declared in ``app.agents``.
        jobs: Discovered jobs.
        installers: Discovered installers.
    """

    models: tuple[type[BaseModel], ...]
    use_cases: tuple[type[UseCase[object, object]], ...]
    interfaces: tuple[type[RestInterface[object]], ...]
    agent_specs: tuple[str, ...] = ()
    jobs: tuple[type[Job[Any]], ...] = ()
    installers: tuple[type[Installer], ...] = ()

    def with_changes(self, **changes: Any) -> DiscoveryResult:
        """Return a copy with *changes* applied, keeping every other field (later ones too)."""
        current = {field.name: getattr(self, field.name) for field in fields(self)}
        return DiscoveryResult(**(current | changes))
