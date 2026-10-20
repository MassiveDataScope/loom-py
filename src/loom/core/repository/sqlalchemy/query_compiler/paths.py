"""Field path resolution for SQLAlchemy models.

Resolves dot-separated field paths (e.g. ``"price"`` or ``"category.name"``)
against a SQLAlchemy mapped class, returning the appropriate column attribute.
"""

from __future__ import annotations

from typing import Any, cast

from loom.core.repository.sqlalchemy.query_compiler.errors import FilterPathError


def resolve_column(sa_model: type[Any], path: str) -> Any:
    """Resolve a dot-separated field path to a SQLAlchemy column attribute.

    Supports single-level paths (``"price"``) and one-level relation traversal
    (``"category.name"``).  Deeper nesting is not supported in v1.

    Args:
        sa_model: Root SQLAlchemy mapped model class.
        path: Dot-separated field path.

    Returns:
        A SQLAlchemy column attribute suitable for use in WHERE / ORDER BY.

    Raises:
        FilterPathError: If any segment of the path cannot be resolved.

    Example::

        col = resolve_column(ProductSA, "price")
        col = resolve_column(ProductSA, "category.name")
    """
    parts = path.split(".", maxsplit=1)

    attr = getattr(sa_model, parts[0], None)
    if attr is None:
        raise FilterPathError(path)

    if len(parts) == 1:
        return attr

    # One-level relation traversal
    related_model = _related_model(attr, path)
    nested = getattr(related_model, parts[1], None)
    if nested is None:
        raise FilterPathError(path)
    return nested


def resolve_relation(sa_model: type[Any], path: str) -> tuple[Any, type[Any]]:
    """Resolve the first path segment as a relationship attribute + its target model.

    Used by the subquery compiler for EXISTS / NOT_EXISTS predicates.

    Args:
        sa_model: Root SQLAlchemy mapped model class.
        path: Dot-separated path whose first segment is a relation name.

    Returns:
        Tuple of ``(relationship_attribute, related_mapped_class)``.

    Raises:
        FilterPathError: If the first segment is not a resolvable relation.
    """
    rel_name = path.split(".", maxsplit=1)[0]
    attr = getattr(sa_model, rel_name, None)
    if attr is None:
        raise FilterPathError(path)

    related = _related_model(attr, path)
    return attr, related


def _related_model(attr: Any, path: str) -> type[Any]:
    """Extract the mapped class from a relationship attribute."""
    try:
        prop = attr.property
        return cast(type[Any], prop.mapper.class_)
    except AttributeError as exc:
        raise FilterPathError(path) from exc
