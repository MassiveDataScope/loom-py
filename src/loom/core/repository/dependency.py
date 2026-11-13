"""How loom reads a constructor annotation as a key, repositories included."""

from __future__ import annotations

from typing import get_origin

from loom.core.di.introspection import KeyDependency, RepoDependency
from loom.core.model.introspection import generic_type_arg
from loom.core.repository.abc import RepoFor
from loom.core.repository.registration import is_direct_protocol, is_standard_capability


def classify_key(annotation: object) -> KeyDependency | RepoDependency | None:
    """``RepoFor[Model]`` is the repository of ``Model``; a class or protocol, a key.

    A parametrised standard capability (``Listable[Order]``) or a protocol
    alias (``Port[Order]``) is a key as written. Any other generic, such as
    ``tuple[str, ...]``, is a value, not a dependency: ``None``.
    """
    model = generic_type_arg(annotation, RepoFor)
    if model is not None:
        return RepoDependency(model)
    return KeyDependency(annotation) if _is_key(annotation) else None


def _is_key(annotation: object) -> bool:
    origin = get_origin(annotation)
    return (
        isinstance(annotation, type)
        or is_standard_capability(annotation)
        or (isinstance(origin, type) and is_direct_protocol(origin))
    )
