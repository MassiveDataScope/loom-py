from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, TypeGuard, TypeVar

import msgspec

from loom.core.model.introspection import (
    extract_model_from_hint,
    get_relations,
    resolve_type_hints,
)

_MISSING = object()


def _related_values(obj: Any, relation: str) -> list[Any]:
    related = getattr(obj, relation, _MISSING)
    if related is _MISSING:
        raise RuntimeError(
            f"Relation loader requires relation '{relation}' on {type(obj).__name__}"
        )
    if related is msgspec.UNSET:
        return []
    if related is None:
        return []
    if isinstance(related, list):
        return related
    return [related]


class ProjectionDescriptor(Protocol):
    """A public projection loader declared on a model class.

    A descriptor states *what* to derive and from which related model; it also
    knows how to build its own memory-path loader, so the compiler never needs
    to branch on the descriptor kind. The SQL-path counterpart is looked up in
    the factory table owned by the SQL repository layer.

    The two attributes are read-only so that a frozen dataclass — what every
    descriptor is — satisfies the protocol.
    """

    @property
    def model(self) -> Any:
        """Related model the projection derives from, or a reference to it."""
        ...

    @property
    def via(self) -> str | None:
        """Relation name to traverse, or ``None`` to infer it."""
        ...

    def build_memory_loader(self, relation: str) -> Any:
        """Return the memory-path loader reading *relation* from a parent object."""
        ...


_DescriptorT = TypeVar("_DescriptorT", bound=ProjectionDescriptor)

_DESCRIPTOR_TYPES: list[type[ProjectionDescriptor]] = []


def projection_descriptor(cls: type[_DescriptorT]) -> type[_DescriptorT]:
    """Register *cls* as a public projection descriptor.

    The registry is the single source of truth for "is this loader a
    descriptor?", which every layer answers through
    :func:`is_projection_descriptor`.

    Registering the same class twice is a no-op.

    Args:
        cls: The descriptor class to register.

    Returns:
        *cls* unchanged, so the call reads as a class decorator.
    """
    if cls not in _DESCRIPTOR_TYPES:
        _DESCRIPTOR_TYPES.append(cls)
    return cls


def projection_descriptor_types() -> tuple[type[ProjectionDescriptor], ...]:
    """Return every registered public projection descriptor class."""
    return tuple(_DESCRIPTOR_TYPES)


def is_projection_descriptor(loader: Any) -> TypeGuard[ProjectionDescriptor]:
    """Return whether *loader* is an instance of a registered descriptor class.

    Args:
        loader: Any projection loader object, descriptor or custom.

    Returns:
        ``True`` for descriptors and their subclasses, ``False`` for custom loaders.
    """
    return isinstance(loader, projection_descriptor_types())


def resolve_model_reference(model_ref: Any) -> type | None:
    """Resolve a descriptor ``model`` reference to a class.

    Accepts a direct class (``CountLoader(model=Note)``) and the zero-argument
    callable used for forward references (``CountLoader(model=lambda: Note)``).

    Args:
        model_ref: The value declared as the descriptor's ``model``.

    Returns:
        The referenced class, or ``None`` when the reference is neither a class
        nor a callable that returns one — including when the callable raises
        because the target model is not importable yet.
    """
    if isinstance(model_ref, type):
        return model_ref
    if callable(model_ref):
        try:
            resolved = model_ref()
        except Exception:
            return None
        return resolved if isinstance(resolved, type) else None
    return None


@projection_descriptor
@dataclass(frozen=True, slots=True)
class CountLoader:
    """Projection loader descriptor: counts related rows per parent entity.

    The compiler resolves the execution strategy at ``compile_all()`` time:
    memory-path when the relation is loaded in the active profile,
    SQL-path otherwise.

    Args:
        model: Target model class whose rows should be counted.
        via: Optional relation attribute name for disambiguation when
            multiple relations target the same model.

    Example::

        count_reviews: int = ProjectionField(
            loader=CountLoader(model=ProductReview),
            profiles=("with_details",),
            default=0,
        )
    """

    model: type
    via: str | None = None

    def build_memory_loader(self, relation: str) -> _MemoryCountLoader:
        """Return the memory-path loader counting *relation* on a parent object."""
        return _MemoryCountLoader(relation=relation)


@projection_descriptor
@dataclass(frozen=True, slots=True)
class ExistsLoader:
    """Projection loader descriptor: checks if related rows exist per parent entity.

    Args:
        model: Target model class to check for existence.
        via: Optional relation attribute name for disambiguation.

    Example::

        has_reviews: bool = ProjectionField(
            loader=ExistsLoader(model=ProductReview),
            profiles=("with_details",),
            default=False,
        )
    """

    model: type
    via: str | None = None

    def build_memory_loader(self, relation: str) -> _MemoryExistsLoader:
        """Return the memory-path loader testing *relation* on a parent object."""
        return _MemoryExistsLoader(relation=relation)


@projection_descriptor
@dataclass(frozen=True, slots=True)
class JoinFieldsLoader:
    """Projection loader descriptor: fetches selected columns from related rows.

    Args:
        model: Target model class to fetch columns from.
        value_columns: Column names to include in each result dict.
        via: Optional relation attribute name for disambiguation.

    Example::

        review_snippets: list[dict] = ProjectionField(
            loader=JoinFieldsLoader(model=ProductReview, value_columns=("id", "rating")),
            profiles=("with_details",),
            default=[],
        )
    """

    model: type
    value_columns: tuple[str, ...]
    via: str | None = None

    def __init__(
        self,
        *,
        model: type,
        value_columns: Sequence[str],
        via: str | None = None,
    ) -> None:
        object.__setattr__(self, "model", model)
        object.__setattr__(self, "value_columns", tuple(value_columns))
        object.__setattr__(self, "via", via)

    def build_memory_loader(self, relation: str) -> _MemoryJoinFieldsLoader:
        """Return the memory-path loader projecting *relation* on a parent object."""
        return _MemoryJoinFieldsLoader(relation=relation, value_columns=self.value_columns)


@dataclass(frozen=True, slots=True)
class _MemoryCountLoader:
    relation: str

    def load_from_object(
        self,
        obj: Any,
        context: Mapping[str, Any] | None = None,
    ) -> int:
        _ = context
        return len(_related_values(obj, self.relation))


@dataclass(frozen=True, slots=True)
class _MemoryExistsLoader:
    relation: str

    def load_from_object(
        self,
        obj: Any,
        context: Mapping[str, Any] | None = None,
    ) -> bool:
        _ = context
        return len(_related_values(obj, self.relation)) > 0


@dataclass(frozen=True, slots=True)
class _MemoryJoinFieldsLoader:
    relation: str
    value_columns: tuple[str, ...]

    def __init__(self, *, relation: str, value_columns: Sequence[str]) -> None:
        object.__setattr__(self, "relation", relation)
        object.__setattr__(self, "value_columns", tuple(value_columns))

    def load_from_object(
        self,
        obj: Any,
        context: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        _ = context
        rows = _related_values(obj, self.relation)
        return [
            {field_name: getattr(row, field_name, None) for field_name in self.value_columns}
            for row in rows
        ]


def make_memory_loader(loader: Any, rel_name: str) -> Any:
    """Convert a public loader descriptor to its memory-path counterpart.

    Args:
        loader: A projection loader descriptor, or any custom loader.
        rel_name: The relation attribute name to read from the object.

    Returns:
        The memory-path loader built by the descriptor, or ``loader`` unchanged
        when it is not a registered descriptor.
    """
    if is_projection_descriptor(loader):
        return loader.build_memory_loader(rel_name)
    return loader


def find_relation_name_for_loader(loader: Any, parent_model: type) -> str | None:
    """Find the relation attribute on ``parent_model`` that matches ``loader.model``.

    If ``loader.via`` is set it is returned directly. Otherwise, the model's
    type hints are scanned for a relation attribute whose annotation resolves
    to ``loader.model`` (unwrapping ``list[X]``, ``X | UnsetType``, etc.).

    Args:
        loader: A projection loader descriptor, or any custom loader.
        parent_model: The domain model class that owns the projection.

    Returns:
        The relation attribute name, or ``None`` if no match is found.
    """
    if not is_projection_descriptor(loader):
        return None
    if loader.via is not None:
        return loader.via

    hints = resolve_type_hints(parent_model)
    if not hints:
        return None

    target_model = loader.model
    for rel_name in get_relations(parent_model):
        hint = hints.get(rel_name)
        if hint is None:
            continue
        if extract_model_from_hint(hint) is target_model:
            return rel_name
    return None
