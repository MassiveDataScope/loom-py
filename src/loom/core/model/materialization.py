"""Marker for loom models that serve introspection without a persisted table."""

from __future__ import annotations

from typing import ClassVar


class NotMaterialized:
    """A loom model with columns for introspection that is never compiled to a table."""

    __materialized__: ClassVar[bool] = False


def is_materialized(model: type) -> bool:
    """Whether a model should be compiled to a table and repository."""
    return bool(getattr(model, "__materialized__", True))
