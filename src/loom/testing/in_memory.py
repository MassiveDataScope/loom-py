"""Generic in-memory repository for use in tests.

Provides :class:`InMemoryRepository` — a fully functional, zero-dependency
fake repository that stores :class:`msgspec.Struct` entities in a plain dict.

Designed to be used with :class:`~loom.testing.http_harness.HttpTestHarness`
or standalone in unit tests, eliminating the need to write a ``FakeXxxRepo``
class for every domain model.

Usage::

    repo = InMemoryRepository(Product, id_field="id")
    repo.seed(Product(id=1, name="Widget"))

    product = await repo.get_by_id(1)   # → Product(id=1, name="Widget")
    missing  = await repo.get_by_id(99) # → None

    created = await repo.create(CreateProductCmd(name="Gadget"))
    # → Product(id=2, name="Gadget")
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any, Generic, TypeVar

import msgspec

from loom.core.config import ConfigError
from loom.core.model import BaseModel
from loom.core.model.introspection import get_id_attributes, get_projections
from loom.core.projection.loaders import (
    find_relation_name_for_loader,
    is_projection_descriptor,
    make_memory_loader,
)
from loom.core.projection.runtime import (
    ProjectionPlan,
    build_projection_plan,
    execute_projection_plan,
)

T = TypeVar("T", bound=msgspec.Struct)


class InMemoryRepository(Generic[T]):
    """Generic in-memory repository for testing any ``msgspec.Struct`` model.

    Stores entities in a plain dict keyed by their id field value.  Provides
    the standard repository surface (``get_by_id``, ``create``, ``update``,
    ``delete``, ``list_paginated``) without any database dependency.

    The ``create`` method derives entity fields from the command automatically
    when no ``creator`` callable is provided: fields present on the command
    are copied to the entity, and the id field is assigned from an internal
    auto-increment counter.

    Args:
        entity_type: The ``msgspec.Struct`` subclass this repository stores.
        id_field: Name of the identity field on the entity.  Defaults to
            ``"id"``.
        creator: Optional ``(cmd, next_id) -> T`` callable used by
            :meth:`create`.  When provided, the automatic field-mapping is
            bypassed entirely.

    Raises:
        ConfigError: If ``entity_type`` is a model with a composite primary key.

    Example::

        repo = InMemoryRepository(Product, id_field="id")
        repo.seed(Product(id=1, name="Widget"), Product(id=2, name="Gadget"))

        harness = HttpTestHarness()
        harness.inject_repo(Product, repo)
        client = harness.build_app(interfaces=[ProductRestInterface])
    """

    def __init__(
        self,
        entity_type: type[T],
        *,
        id_field: str = "id",
        creator: Callable[[Any, int], T] | None = None,
    ) -> None:
        _refuse_composite_key(entity_type)
        self._entity_type = entity_type
        self._id_field = id_field
        self._creator = creator
        self._store: dict[Any, T] = {}
        self._next_id: int = 1
        self._projection_plans: dict[str, ProjectionPlan | None] = {}

    def seed(self, *entities: T) -> None:
        """Pre-load entities into the store.

        The internal id counter is advanced past the highest integer id seen
        so that subsequent :meth:`create` calls do not collide.

        Args:
            *entities: Entity instances to load.

        Example::

            repo.seed(Product(id=1, name="A"), Product(id=2, name="B"))
        """
        for entity in entities:
            id_val = getattr(entity, self._id_field)
            self._store[id_val] = entity
            if isinstance(id_val, int) and id_val >= self._next_id:
                self._next_id = id_val + 1

    async def get_by_id(self, obj_id: Any, profile: str = "default") -> T | None:
        """Return the entity with ``obj_id``, or ``None`` if not found.

        Args:
            obj_id: The identity value to look up.
            profile: Ignored; present for repository interface compatibility.

        Returns:
            Entity instance, or ``None`` if no entity has that id.
        """
        entity = self._store.get(obj_id)
        if entity is None:
            return None
        return await self._with_projections(entity, profile=profile)

    async def create(self, cmd: Any) -> T:
        """Create and store a new entity from ``cmd``.

        If a ``creator`` callable was provided at construction it is called as
        ``creator(cmd, next_id)``.  Otherwise, command attributes whose names
        match entity fields are copied automatically, and the id field is set
        from the internal auto-increment counter.

        Args:
            cmd: Command or payload object carrying the new entity's data.

        Returns:
            The created and stored entity.
        """
        if self._creator is not None:
            entity = self._creator(cmd, self._next_id)
            self._next_id += 1
        else:
            entity = self._auto_create(cmd)
        id_val = getattr(entity, self._id_field)
        self._store[id_val] = entity
        return entity

    async def update(self, obj_id: Any, data: Any) -> T | None:
        """Update the entity at ``obj_id`` with fields from ``data``.

        Only non-``None`` fields present on both ``data`` and the entity are
        overwritten; the id field is never changed.

        Args:
            obj_id: Identity value of the entity to update.
            data: Object or dict with updated field values.

        Returns:
            The updated entity, or ``None`` if no entity has that id.
        """
        entity = self._store.get(obj_id)
        if entity is None:
            return None
        current = msgspec.structs.asdict(entity)
        updates: dict[str, Any] = (
            data
            if isinstance(data, dict)
            else {f.name: getattr(data, f.name) for f in msgspec.structs.fields(type(data))}
        )
        for k, v in updates.items():
            if v is not None and k in current and k != self._id_field:
                current[k] = v
        updated = msgspec.convert(current, self._entity_type)
        self._store[obj_id] = updated
        return updated

    async def delete(self, obj_id: Any) -> bool:
        """Delete the entity at ``obj_id``.

        Args:
            obj_id: Identity value to delete.

        Returns:
            ``True`` if the entity existed and was removed, ``False`` if not
            found.
        """
        if obj_id in self._store:
            del self._store[obj_id]
            return True
        return False

    async def list_paginated(self, *args: Any, **kwargs: Any) -> list[T]:
        """Return all stored entities.

        Args:
            *args: Ignored; present for repository interface compatibility.
            **kwargs: Ignored; present for repository interface compatibility.

        Returns:
            List of all entities in insertion order.
        """
        profile = kwargs.get("profile", "default")
        entities = list(self._store.values())
        if not entities:
            return []

        plan = self._projection_plan_for_profile(profile)
        if plan is None:
            return entities

        projection_values = await execute_projection_plan(
            plan,
            objs=entities,
            id_attr=self._id_field,
            backend_context=None,
        )
        return [
            self._apply_projection_values(entity, projection_values.get(i))
            for i, entity in enumerate(entities)
        ]

    def _auto_create(self, cmd: Any) -> T:
        """Derive entity from command attributes and auto-increment id."""
        entity_fields = {f.name for f in msgspec.structs.fields(self._entity_type)}
        data: dict[str, Any] = {}
        for f_name in entity_fields:
            if f_name == self._id_field:
                data[f_name] = self._next_id
            elif hasattr(cmd, f_name):
                data[f_name] = getattr(cmd, f_name)
        self._next_id += 1
        return msgspec.convert(data, self._entity_type)

    def _projection_plan_for_profile(self, profile: str) -> ProjectionPlan | None:
        if profile in self._projection_plans:
            return self._projection_plans[profile]

        model_projections = get_projections(self._entity_type)
        active = {
            name: projection
            for name, projection in model_projections.items()
            if profile in projection.profiles
        }
        if not active:
            self._projection_plans[profile] = None
            return None

        resolved = self._resolve_memory_loaders(active)
        compiled = build_projection_plan(resolved)
        self._projection_plans[profile] = compiled
        return compiled

    def _resolve_memory_loaders(self, projections: dict[str, Any]) -> dict[str, Any]:
        """Replace public loader descriptors with memory-path loaders."""
        result: dict[str, Any] = {}
        for name, proj in projections.items():
            loader = proj.loader
            if is_projection_descriptor(loader):
                rel_name = find_relation_name_for_loader(loader, self._entity_type)
                if rel_name is None:
                    raise ValueError(
                        f"InMemoryRepository: cannot resolve '{name}' loader "
                        f"({type(loader).__name__}(model={loader.model.__name__})). "
                        "Provide 'via' on the loader or annotate the relation with the target type."
                    )
                result[name] = replace(proj, loader=make_memory_loader(loader, rel_name))
            else:
                result[name] = proj
        return result

    async def _with_projections(self, entity: T, *, profile: str) -> T:
        plan = self._projection_plan_for_profile(profile)
        if plan is None:
            return entity

        result = await execute_projection_plan(
            plan,
            objs=[entity],
            id_attr=self._id_field,
            backend_context=None,
        )
        return self._apply_projection_values(entity, result.get(0))

    def _apply_projection_values(self, entity: T, values: dict[str, Any] | None) -> T:
        if not values:
            return entity
        data = msgspec.structs.asdict(entity)
        data.update(values)
        return self._entity_type(**data)


def _refuse_composite_key(entity_type: type) -> None:
    if not issubclass(entity_type, BaseModel):
        return
    try:
        key = get_id_attributes(entity_type)
    except ValueError:  # a model with no key, which this repository accepts
        return
    if len(key) > 1:
        raise ConfigError(
            f"{entity_type.__qualname__} has the composite primary key ({', '.join(key)}); "
            "the in-memory repository keys each entity by one id field, so declare a "
            "single primary key"
        )
