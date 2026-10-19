"""Primary-key generation and ``_id`` storage forms for the Mongo repository.

MongoDB has no autoincrement, so loom generates the key on ``create`` /
``create_many`` when the input carries none. Which value is generated and how
a model-level id is stored in ``_id`` is an :class:`IdPolicy`; the two
built-in policies are :class:`Uuid4IdPolicy` (default) and
:class:`ObjectIdPolicy` (opt-in for collections already keyed by
``ObjectId``).

``bson`` ships with ``pymongo`` (``loom-kernel[mongo]``). It is imported at
module level on purpose: this module is only reached through the Mongo
backend, and the persistence registry already turns an ``ImportError`` at
entry-point load into a ``ConfigError`` naming the extra.
"""

from __future__ import annotations

from typing import Protocol
from uuid import UUID, uuid4

from bson import ObjectId

from loom.core.config import ConfigError
from loom.core.model.introspection import ColumnFieldInfo, get_column_fields

# Annotations that can hold the key either built-in policy generates.
_GENERATED_KEY_TYPES = (str, UUID)


class IdPolicy(Protocol):
    """How a repository mints and stores primary keys in ``_id``.

    A client-supplied id always wins: :meth:`generate` is called only when
    the input struct carries no primary-key value.
    """

    def generate(self) -> object:
        """Return a fresh key in its ``_id`` storage form."""
        ...

    def to_storage(self, value: object) -> object:
        """Convert a model-level id into the value stored in ``_id``."""
        ...

    def from_storage(self, value: object) -> object:
        """Convert an ``_id`` value back into the model-level id."""
        ...


class Uuid4IdPolicy:
    """Stores ids as UUID version 4 strings.

    A model may annotate its primary key as ``str`` or ``UUID``: the value
    is written to ``_id`` as its string form either way and the output
    struct's annotation restores the type on read.

    Example::

        repository = RepositoryMongo(
            Article, collection, id_policy=Uuid4IdPolicy(), cursor_codec=codec
        )
    """

    def generate(self) -> str:
        """Return a new UUID4 as a string."""
        return str(uuid4())

    def to_storage(self, value: object) -> object:
        """Return ``value`` as a string when it is a :class:`UUID`, unchanged otherwise."""
        return str(value) if isinstance(value, UUID) else value

    def from_storage(self, value: object) -> object:
        """Return the stored value unchanged; the struct annotation restores the type."""
        return value


class ObjectIdPolicy:
    """Stores ids as BSON ``ObjectId`` and exposes them as strings.

    Opt in for collections whose documents are already keyed by
    ``ObjectId``. The model declares its primary key as ``str``; a lookup
    with a string that is not a valid ``ObjectId`` matches nothing.

    Example::

        repository = RepositoryMongo(
            Article, collection, id_policy=ObjectIdPolicy(), cursor_codec=codec
        )
    """

    def generate(self) -> ObjectId:
        """Return a new ``ObjectId``."""
        return ObjectId()

    def to_storage(self, value: object) -> object:
        """Return ``value`` as an ``ObjectId`` when it is a valid hex string."""
        if isinstance(value, str) and ObjectId.is_valid(value):
            return ObjectId(value)
        return value

    def from_storage(self, value: object) -> object:
        """Return an ``ObjectId`` as its 24-character hex string."""
        return str(value) if isinstance(value, ObjectId) else value


def validate_id_field(model: type) -> ColumnFieldInfo:
    """Return the primary-key field of ``model`` after checking it suits Mongo.

    Args:
        model: Loom model bound to a collection.

    Returns:
        Metadata of the primary-key field.

    Raises:
        ConfigError: If the model has no primary key, its primary key
            declares ``autoincrement=True`` (MongoDB has no sequences; use
            ``ServerDefault.UUID4`` or ``ObjectIdPolicy``), or its primary
            key is annotated with a type other than ``str`` or ``UUID``,
            which could not hold the id the policy generates.
    """
    for info in get_column_fields(model).values():
        if not info.field.primary_key:
            continue
        _reject_autoincrement(model, info)
        _reject_unmintable_annotation(model, info)
        return info
    raise ConfigError(f"{model.__qualname__} declares no primary key.")


def _reject_autoincrement(model: type, info: ColumnFieldInfo) -> None:
    if not info.field.autoincrement:
        return
    raise ConfigError(
        f"{model.__qualname__}.{info.name} declares autoincrement=True, which the "
        "mongo backend cannot serve; declare server_default=ServerDefault.UUID4 "
        "or opt in to ObjectId ids."
    )


def _reject_unmintable_annotation(model: type, info: ColumnFieldInfo) -> None:
    if info.python_type in _GENERATED_KEY_TYPES:
        return
    raise ConfigError(
        f"{model.__qualname__}.{info.name} is annotated {info.python_type.__name__}, which "
        "cannot hold the id the mongo backend generates; annotate the key as str or UUID."
    )


__all__ = ["IdPolicy", "ObjectIdPolicy", "Uuid4IdPolicy", "validate_id_field"]
