"""Which ``database`` section a model uses.

A model with no ``__database__`` uses the ``database`` section. A name uses
``database.<name>``, the same section a streaming sink selects with its
``database`` key. Nothing here opens a connection.
"""

from __future__ import annotations

from collections.abc import Sequence

from loom.core.config import ConfigError
from loom.core.model import BaseModel

_RESERVED = frozenset(
    {
        "url",
        "username",
        "password",
        "echo",
        "pool",
        "connect_args",
        "schema",
        "connect_timeout",
        "read_timeout",
    }
)
"""Keys of the ``database`` section. They are not names of another database."""


def database_of(model: type[BaseModel]) -> str | None:
    """Return the model's ``__database__`` name, or ``None`` for the default section.

    Raises:
        ConfigError: When the attribute is set and is not a section name.
    """
    raw = getattr(model, "__database__", None)
    if raw is None:
        return None
    if isinstance(raw, str) and raw.isidentifier() and raw not in _RESERVED:
        return raw
    raise ConfigError(f"{model.__qualname__}.__database__ must name a database.<name> section")


def first_named_database(
    models: Sequence[type[BaseModel]],
) -> tuple[type[BaseModel], str] | None:
    """Return the first model that names a ``database.<name>`` section.

    Raises:
        ConfigError: When a ``__database__`` value is not a section name.
            No engine is built.
    """
    for model in models:
        name = database_of(model)
        if name is not None:
            return model, name
    return None


def group_models(
    models: Sequence[type[BaseModel]],
) -> tuple[tuple[str | None, tuple[type[BaseModel], ...]], ...]:
    """Group models by :func:`database_of`, the default group first.

    Raises:
        ConfigError: When two models on different databases share a table name,
            or a ``__database__`` value is not a section name. No engine is built.
    """
    seen: dict[str, type[BaseModel]] = {}
    buckets: dict[str | None, list[type[BaseModel]]] = {}
    order: list[str | None] = []
    for model in models:
        name = database_of(model)
        table = getattr(model, "__tablename__", None)
        if isinstance(table, str):
            previous = seen.get(table)
            if previous is not None and database_of(previous) != name:
                raise ConfigError(
                    f"table {table!r} is declared by {previous.__qualname__} and "
                    f"{model.__qualname__} on different databases"
                )
            seen.setdefault(table, model)
        if name not in buckets:
            buckets[name] = []
            order.append(name)
        buckets[name].append(model)
    if None in buckets:
        order.remove(None)
        order.insert(0, None)
    return tuple((name, tuple(buckets[name])) for name in order)
