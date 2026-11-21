"""Generic in-memory repository for use in tests.

Provides :class:`InMemoryRepository` — a fully functional, zero-dependency
fake repository that stores the entities of a loom model in a plain dict, keyed
by the primary key the model declares.

Designed to be used with :class:`~loom.testing.http_harness.HttpTestHarness`
or standalone in unit tests, eliminating the need to write a ``FakeXxxRepo``
class for every domain model.

Usage::

    repo = InMemoryRepository(Product)
    repo.seed(Product(id=1, name="Widget"))

    product = await repo.get_by_id(1)   # → Product(id=1, name="Widget")
    missing  = await repo.get_by_id(99) # → None

    created = await repo.create(CreateProductCmd(name="Gadget"))
    # → Product(id=2, name="Gadget")
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar

import msgspec

from loom.core.errors import Conflict
from loom.core.model.convert import to_struct
from loom.core.model.introspection import business_key_of, get_id_attributes, get_projections
from loom.core.projection.loaders import (
    build_memory_loader,
    find_relation_name_for_loader,
    is_projection_descriptor,
)
from loom.core.projection.plan import (
    ProjectionPlan,
    build_projection_plan,
    execute_projection_plan,
)
from loom.core.stores.repository import (
    BulkCreatable,
    Creatable,
    Deletable,
    OnConflict,
    Readable,
    Updatable,
)
from loom.core.stores.repository.record import (
    PrimaryKey,
    RecordLayout,
    ServerStamps,
    refuse_key_change,
    with_derived_key,
)

T = TypeVar("T", bound=msgspec.Struct)


class InMemoryRepository(
    Readable[T], Creatable[T], BulkCreatable[T], Updatable[T], Deletable[T], Generic[T]
):
    """Generic in-memory repository for testing any model that declares a primary key.

    Stores entities in a plain dict keyed by their primary-key values.  Provides
    the standard repository surface (``get_by_id``, ``create``, ``update``,
    ``delete``, ``list_paginated``) without any database dependency.

    The ``create`` method derives entity fields from the command automatically
    when no ``creator`` callable is provided: fields present on the command
    are copied to the entity, and the id field is the one the model derives from
    its business key, else the command's when it carries one, else an internal
    auto-increment counter's; a ``creator`` fills the id itself. As the SQL store's
    server defaults do, a ``NOW`` field the command, or the ``creator``'s entity,
    leaves unset or ``None`` holds the current UTC time, and every update stamps
    the ``NOW`` on-update fields.
    Creating a primary key or a business key the store already holds raises
    ``Conflict``, as every real repository does, and neither ``create`` nor
    ``create_many`` overwrites a row.
    ``create_many`` follows the port and the SQLAlchemy adapter: with ``ERROR`` a
    repeated or existing key raises ``Conflict`` and nothing is stored; with ``IGNORE``
    a row is skipped only when its business key (its primary key when the model
    declares none) is already stored or came earlier in the batch, and any other
    collision, a primary key included, still raises ``Conflict`` and stores nothing.
    The key is the primary key the model declares, single or composite; a
    composite key is an id tuple in key order, as in the SQLAlchemy adapter.
    It declares the key capabilities (``Readable``, ``Creatable``, ``BulkCreatable``,
    ``Updatable``, ``Deletable``); ``list_paginated`` is a convenience that
    returns every entity, not the ``Listable`` contract.

    Args:
        entity_type: The model this repository stores; its declared primary key
            is the store key.
        creator: Optional ``(cmd, next_id) -> T`` callable used by
            :meth:`create`.  When provided, the automatic field-mapping is
            bypassed; the server stamps still fill the entity's empty ``NOW`` fields.

    Example::

        repo = InMemoryRepository(Product)
        repo.seed(Product(id=1, name="Widget"), Product(id=2, name="Gadget"))

        harness = HttpTestHarness()
        harness.inject_repo(Product, repo)
        client = harness.build_app(interfaces=[ProductRestInterface])

    Raises:
        ValueError: If *entity_type* declares no primary-key field.
    """

    def __init__(
        self,
        entity_type: type[T],
        *,
        creator: Callable[[Any, int], T] | None = None,
    ) -> None:
        self._entity_type = entity_type
        self._key = PrimaryKey(entity_type.__name__, get_id_attributes(entity_type))
        business_key = business_key_of(entity_type)
        self._business_key_fields = () if business_key is None else business_key.fields
        self._creator = creator
        self._stamps = ServerStamps.of(entity_type)
        self._store: dict[tuple[Any, ...], T] = {}
        self._key_by_business_key: dict[tuple[Any, ...], tuple[Any, ...]] = {}
        self._next_id: int = 1
        self._projection_plans: dict[str, ProjectionPlan | None] = {}

    def seed(self, *entities: T) -> None:
        """Pre-load entities into the store.

        The internal id counter is advanced past the highest integer single-field
        id seen so that subsequent :meth:`create` calls do not collide.

        Args:
            *entities: Entity instances to load.

        Example::

            repo.seed(Product(id=1, name="A"), Product(id=2, name="B"))
        """
        for entity in entities:
            self._put(entity)
            obj_id = self._key.id_of(entity)
            if isinstance(obj_id, int) and obj_id >= self._next_id:
                self._next_id = obj_id + 1

    async def get_by_id(self, obj_id: Any, profile: str = "default") -> T | None:
        """Return the entity with ``obj_id``, or ``None`` if not found.

        Args:
            obj_id: The identity value to look up; a tuple of every key value, in
                key order, when the primary key is composite.
            profile: Ignored; present for repository interface compatibility.

        Returns:
            Entity instance, or ``None`` if no entity has that id.
        """
        entity = self._store.get(self._key.values_of(obj_id))
        if entity is None:
            return None
        return await self._with_projections(entity, profile=profile)

    async def create(self, data: Any) -> T:
        """Create and store a new entity from ``data``.

        If a ``creator`` callable was provided at construction it is called as
        ``creator(data, next_id)``.  Otherwise, command attributes whose names
        match entity fields are copied automatically, and a single-field primary
        key the command leaves unset comes from the internal auto-increment counter.

        Args:
            data: Command or payload object carrying the new entity's data.

        Returns:
            The created and stored entity.

        Raises:
            Conflict: When the entity's primary key or business key is already stored.
        """
        entity = self._build_entity(data)
        self._refuse_collisions([entity])
        self._put(entity)
        return entity

    async def create_many(
        self, data: Sequence[Any], *, on_conflict: OnConflict = OnConflict.ERROR
    ) -> tuple[T, ...]:
        """Create and store the entities of *data* in one step.

        Args:
            data: Command or payload objects, as :meth:`create` takes.
            on_conflict: ``ERROR`` raises on a key that is stored or repeated;
                ``IGNORE`` skips the rows whose business key (primary key when
                the model declares none) is stored or came earlier in *data*.

        Returns:
            The stored entities in input order: every one with ``ERROR``, only
            the inserted ones with ``IGNORE``.

        Raises:
            Conflict: When a primary key or business key collides outside the
                rows ``IGNORE`` skips; nothing is stored.
        """
        entities = [self._build_entity(item) for item in data]
        if on_conflict is OnConflict.IGNORE:
            entities = self._without_known_targets(entities)
        self._refuse_collisions(entities)
        for entity in entities:
            self._put(entity)
        return tuple(entities)

    async def update(self, obj_id: Any, data: Any) -> T | None:
        """Update the entity at ``obj_id`` with fields from ``data``.

        Only non-``None``, non-``UNSET`` fields present on both ``data`` and the
        entity are overwritten, and a field a command leaves at its default is not
        sent; the id field is never changed. The ``NOW`` on-update fields hold the
        current UTC time, whatever ``data`` sends for them.

        Args:
            obj_id: Identity value of the entity to update, as in :meth:`get_by_id`.
            data: Object or dict with updated field values.

        Returns:
            The updated entity, or ``None`` if no entity has that id.
        """
        store_key = self._key.values_of(obj_id)
        entity = self._store.get(store_key)
        if entity is None:
            return None
        current = msgspec.structs.asdict(entity)
        updates = data if isinstance(data, dict) else _sent_values(data)
        changes = {
            k: v
            for k, v in updates.items()
            if v is not None
            and v is not msgspec.UNSET
            and k in current
            and k not in self._key.names
        }
        refuse_key_change(self._entity_type, changes)
        current.update(changes)
        updated = to_struct(
            self._entity_type, self._stamps.with_update_stamps(current, datetime.now(UTC))
        )
        self._put(updated)
        return updated

    async def delete(self, obj_id: Any) -> bool:
        """Delete the entity at ``obj_id``.

        Args:
            obj_id: Identity value to delete, as in :meth:`get_by_id`.

        Returns:
            ``True`` if the entity existed and was removed, ``False`` if not
            found.
        """
        removed = self._store.pop(self._key.values_of(obj_id), None)
        if removed is None:
            return False
        self._key_by_business_key.pop(self._business_key_of(removed), None)
        return True

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
            id_attr=self._key.names[0],
            backend_context=None,
        )
        return [
            self._apply_projection_values(entity, projection_values.get(i))
            for i, entity in enumerate(entities)
        ]

    def _build_entity(self, data: Any) -> T:
        """Build the entity *data* creates, with the server stamps a store fills."""
        if self._creator is None:
            values = self._command_values(data)
        else:
            values = self._creator_values(self._creator, data)
        stamped = self._stamps.with_create_stamps(values, datetime.now(UTC))
        return to_struct(self._entity_type, stamped)

    def _creator_values(self, creator: Callable[[Any, int], T], data: Any) -> dict[str, Any]:
        """Return the field values of the entity *creator* builds, ``UNSET`` left out."""
        entity = creator(data, self._next_id)
        self._next_id += 1
        return RecordLayout.from_struct(type(entity)).to_values(entity)

    def _business_key_of(self, entity: T) -> tuple[Any, ...]:
        return tuple(getattr(entity, name) for name in self._business_key_fields)

    def _ignore_target_of(self, entity: T) -> tuple[Any, ...]:
        """Return what ``IGNORE`` compares: the business key, else the primary key."""
        if self._business_key_fields:
            return self._business_key_of(entity)
        return self._key.values_in(entity)

    def _is_stored_target(self, target: tuple[Any, ...]) -> bool:
        if self._business_key_fields:
            return target in self._key_by_business_key
        return target in self._store

    def _put(self, entity: T) -> None:
        """Store *entity* under its primary key and index its business key."""
        key = self._key.values_in(entity)
        if not self._business_key_fields:
            self._store[key] = entity
            return
        previous = self._store.get(key)
        if previous is not None:
            del self._key_by_business_key[self._business_key_of(previous)]
        self._store[key] = entity
        self._key_by_business_key[self._business_key_of(entity)] = key

    def _without_known_targets(self, entities: list[T]) -> list[T]:
        seen: set[tuple[Any, ...]] = set()
        fresh = []
        for entity in entities:
            target = self._ignore_target_of(entity)
            if target not in seen and not self._is_stored_target(target):
                seen.add(target)
                fresh.append(entity)
        return fresh

    def _refuse_collisions(self, entities: list[T]) -> None:
        """Refuse *entities* that repeat a primary key or business key, or hold a stored one.

        Raises:
            Conflict: On the first collision; nothing has been stored.
        """
        keys: set[tuple[Any, ...]] = set()
        business_keys: set[tuple[Any, ...]] = set()
        for entity in entities:
            key = self._key.values_in(entity)
            if key in keys or key in self._store:
                raise Conflict(
                    f"{self._entity_type.__name__} with "
                    f"{', '.join(self._key.names)}={key!r} already exists."
                )
            keys.add(key)
            if not self._business_key_fields:
                continue
            business_key = self._business_key_of(entity)
            if business_key in business_keys or business_key in self._key_by_business_key:
                raise Conflict(
                    f"{self._entity_type.__name__} with "
                    f"{', '.join(self._business_key_fields)}={business_key!r} already exists."
                )
            business_keys.add(business_key)

    def _command_values(self, cmd: Any) -> dict[str, Any]:
        """Return the entity values *cmd* carries, the derived or auto-increment id filled in."""
        entity_fields = {f.name for f in msgspec.structs.fields(self._entity_type)}
        data: dict[str, Any] = {}
        for f_name in entity_fields:
            value = getattr(cmd, f_name, msgspec.UNSET)
            if value is not msgspec.UNSET:
                data[f_name] = value
        data = with_derived_key(self._entity_type, data)
        match self._key.names:
            case (name,) if data.get(name) is None:
                data[name] = self._next_id
        self._next_id += 1
        return data

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
                result[name] = replace(proj, loader=build_memory_loader(loader, rel_name))
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
            id_attr=self._key.names[0],
            backend_context=None,
        )
        return self._apply_projection_values(entity, result.get(0))

    def _apply_projection_values(self, entity: T, values: dict[str, Any] | None) -> T:
        if not values:
            return entity
        data = msgspec.structs.asdict(entity)
        data.update(values)
        return self._entity_type(**data)


def _sent_values(data: msgspec.Struct) -> dict[str, Any]:
    """The fields of *data* by name, leaving out those a command at its default omits."""
    omits_defaults = type(data).__struct_config__.omit_defaults
    sent: dict[str, Any] = {}
    for field in msgspec.structs.fields(type(data)):
        value = getattr(data, field.name)
        if omits_defaults and value == _default_of(field):
            continue
        sent[field.name] = value
    return sent


def _default_of(field: msgspec.structs.FieldInfo) -> Any:
    """The default of *field*, or ``NODEFAULT`` when it has none."""
    if field.default_factory is not msgspec.NODEFAULT:
        return field.default_factory()
    return field.default
