"""EXISTS / NOT_EXISTS subquery generation for relationship fields."""

from __future__ import annotations

from typing import Any

from sqlalchemy import exists, select

from loom.stores.sqlalchemy.query_compiler.paths import resolve_relation


def compile_exists(sa_model: type[Any], relation_path: str) -> Any:
    """Build an EXISTS subquery for a relationship path.

    Args:
        sa_model: Root SQLAlchemy mapped model class.
        relation_path: Dot-separated path whose first segment names the
            relationship (e.g. ``"reviews"``).

    Returns:
        A SQLAlchemy EXISTS clause element.

    Raises:
        FilterPathError: If the relation path cannot be resolved.
    """
    _, related = resolve_relation(sa_model, relation_path)
    return exists(select(related).correlate(sa_model)).correlate(sa_model)


def compile_not_exists(sa_model: type[Any], relation_path: str) -> Any:
    """Build a NOT EXISTS subquery for a relationship path.

    Args:
        sa_model: Root SQLAlchemy mapped model class.
        relation_path: Dot-separated path whose first segment names the
            relationship.

    Returns:
        A SQLAlchemy ``~EXISTS`` clause element.

    Raises:
        FilterPathError: If the relation path cannot be resolved.
    """
    return ~compile_exists(sa_model, relation_path)
