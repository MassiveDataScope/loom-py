from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import msgspec

from loom.core.repository import get_repository_registration
from loom.core.repository.abc import CursorCodec
from loom.core.repository.registry import RepositoryBuildContext
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager

ScenarioDict = dict[str, list[msgspec.Struct]]


@dataclass(slots=True)
class EntityHarness:
    repository: Any
    service: Any | None = None


class RepositoryIntegrationHarness:
    """Test harness for integration tests over repository implementations."""

    def __init__(
        self,
        *,
        session_manager: SessionManager,
        entities: Mapping[str, EntityHarness],
        load_order: tuple[str, ...] = (),
    ) -> None:
        self.session_manager = session_manager
        self._entities: dict[str, EntityHarness] = dict(entities)
        self._load_order = load_order
        for entity_name, context in self._entities.items():
            setattr(self, entity_name, context)

    def __getattr__(self, name: str) -> EntityHarness:
        entity = self._entities.get(name)
        if entity is None:
            raise AttributeError(name)
        return entity

    async def load(self, scenario: ScenarioDict) -> None:
        execution_order: list[str] = [name for name in self._load_order if name in scenario]
        execution_order.extend(name for name in scenario if name not in execution_order)

        for entity_name in execution_order:
            entity_context = self._entities.get(entity_name)
            if entity_context is None:
                raise ValueError(
                    f"Entity '{entity_name}' is not registered in RepositoryIntegrationHarness"
                )
            for contract in scenario.get(entity_name, []):
                await entity_context.repository.create(contract)


def build_repository_harness(
    *,
    session_manager: SessionManager,
    cursor_codec: CursorCodec,
    models: Mapping[str, type[Any]],
    repositories: Mapping[str, Any] | None = None,
    load_order: tuple[str, ...] = (),
) -> RepositoryIntegrationHarness:
    """Build a repository integration harness with generic/default repositories.

    Args:
        session_manager: Shared SQLAlchemy session manager.
        cursor_codec: Codec the default repositories issue and verify cursors
            with; ``CursorCodec(signing="ephemeral")`` suits most tests.
        models: Mapping ``entity_key -> model class``.
        repositories: Optional mapping ``entity_key -> repository instance``.
        load_order: Optional seed load order.

    Returns:
        Configured repository integration harness.
    """
    entities: dict[str, EntityHarness] = {}
    for entity_name, model in models.items():
        repository = None
        if repositories is not None:
            repository = repositories.get(entity_name)
        if repository is None:
            registration = get_repository_registration(model)
            if registration is not None and registration.builder is not None:
                repository = registration.builder(RepositoryBuildContext(model=model))
            else:
                repository_type = (
                    registration.repository_type
                    if registration is not None
                    else RepositorySQLAlchemy
                )
                repository = repository_type(
                    session_manager=session_manager, model=model, cursor_codec=cursor_codec
                )
        entities[entity_name] = EntityHarness(repository=repository)

    return RepositoryIntegrationHarness(
        session_manager=session_manager,
        entities=entities,
        load_order=load_order,
    )
