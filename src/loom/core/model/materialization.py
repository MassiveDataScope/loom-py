"""Marker for loom models that serve introspection without a persisted table."""

from __future__ import annotations

from collections.abc import Iterable
from typing import ClassVar, TypeVar

ModelT = TypeVar("ModelT", bound=type)


class NotMaterialized:
    """A loom model with columns for introspection that is never compiled to a table."""

    __materialized__: ClassVar[bool] = False


def is_materialized(model: type) -> bool:
    """Whether a model should be compiled to a table and repository."""
    return bool(getattr(model, "__materialized__", True))


def materialized_models(models: Iterable[ModelT]) -> tuple[ModelT, ...]:
    """The models of *models* that are compiled to tables, in their order."""
    return tuple(model for model in models if is_materialized(model))
