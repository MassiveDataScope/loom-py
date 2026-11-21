"""The values a struct persists when it is created, and the key it persists under."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar

import msgspec

from loom.core.config import ConfigError
from loom.core.model.enums import ServerDefault, ServerOnUpdate
from loom.core.model.introspection import derived_key_of, get_column_fields, get_id_attributes


class RecordLayout:
    """The fields of one struct type that can hold ``msgspec.UNSET``, and its tag.

    A layout is built once per struct type; :meth:`from_struct` returns the same
    object for the same type.
    """

    _layouts: ClassVar[dict[type[msgspec.Struct], RecordLayout]] = {}

    def __init__(self, struct_type: type[msgspec.Struct]) -> None:
        config = struct_type.__struct_config__
        self._tag = None if config.tag_field is None else (config.tag_field, config.tag)
        self._unsettable = tuple(
            field.name
            for field in msgspec.structs.fields(struct_type)
            if field.default is msgspec.UNSET or self._admits_unset(field.type)
        )

    @staticmethod
    def _admits_unset(annotation: Any) -> bool:
        """Whether *annotation* names ``msgspec.UnsetType`` or a union holding it."""
        if annotation is msgspec.UnsetType:
            return True
        return any(
            RecordLayout._admits_unset(member) for member in getattr(annotation, "__args__", ())
        )

    @classmethod
    def from_struct(cls, struct_type: type[msgspec.Struct]) -> RecordLayout:
        """Return the layout of *struct_type*, building it on first use."""
        layout = cls._layouts.get(struct_type)
        if layout is None:
            layout = cls._layouts[struct_type] = cls(struct_type)
        return layout

    def to_values(self, row: msgspec.Struct) -> dict[str, Any]:
        """Return the field values of *row* keyed by field name, ``UNSET`` fields left out.

        A field left at its default is included, whatever ``omit_defaults``
        the struct declares, and a tagged struct carries its tag field. Values
        are not converted.
        """
        values = msgspec.structs.asdict(row)
        for name in self._unsettable:
            if values[name] is msgspec.UNSET:
                del values[name]
        if self._tag is not None:
            values[self._tag[0]] = self._tag[1]
        return values


def with_derived_key(model: type, values: dict[str, Any]) -> dict[str, Any]:
    """Return *values* with the id *model* derives from its business key.

    The one place a repository fills a derived id: every create path passes
    its values through here. A model that derives no key gets *values* back.

    Raises:
        BoundaryValidationError: If *values* carries an id other than the
            derived one, or lacks a business-key field.
    """
    derived = derived_key_of(model)
    return values if derived is None else derived.complete(values)


def creation_values(model: type, row: msgspec.Struct) -> dict[str, Any]:
    """Return the values a repository persists when it creates *model* from *row*.

    These are :meth:`RecordLayout.to_values` of the row, with the derived id
    filled in by :func:`with_derived_key`.
    """
    return with_derived_key(model, RecordLayout.from_struct(type(row)).to_values(row))


def refuse_key_change(model: type, written: Iterable[str]) -> None:
    """Refuse an update of *model* that writes one of the *written* field names of a derived key.

    Raises:
        BoundaryValidationError: If *model* derives its id and *written* names
            a business-key field.
    """
    derived = derived_key_of(model)
    if derived is not None:
        derived.refuse_change(written)


@dataclass(frozen=True, slots=True)
class ServerStamps:
    """The timestamp fields a store without server defaults stamps itself.

    Each stamping gives every field it fills one shared instant, *now*, which the
    store reads from its clock in its storage form.

    Attributes:
        on_create: Fields stamped with the current UTC time when a create leaves them empty.
        on_update: Fields stamped with the current UTC time on every update.
    """

    on_create: frozenset[str]
    on_update: frozenset[str]

    @classmethod
    def of(cls, model: type) -> ServerStamps:
        """Read the stamped fields from the model's ``NOW`` server defaults and updates."""
        fields = get_column_fields(model)
        return cls(
            on_create=frozenset(
                name
                for name, info in fields.items()
                if info.field.server_default is ServerDefault.NOW
            ),
            on_update=frozenset(
                name
                for name, info in fields.items()
                if ServerOnUpdate.is_now(info.field.server_onupdate)
            ),
        )

    def with_create_stamps(self, values: dict[str, Any], now: datetime) -> dict[str, Any]:
        """Return a copy of *values* with each ``on_create`` field it leaves empty holding *now*.

        A field is empty when *values* lacks it or holds ``None``; a supplied
        value is kept.
        """
        empty = (name for name in self.on_create if values.get(name) is None)
        return {**values, **dict.fromkeys(empty, now)}

    def with_update_stamps(self, changes: dict[str, Any], now: datetime) -> dict[str, Any]:
        """Return a copy of *changes* with every ``on_update`` field holding *now*.

        A value *changes* sends for an ``on_update`` field is overwritten.
        """
        return {**changes, **dict.fromkeys(self.on_update, now)}


def single_id_attribute(model: type, *, why_single: str) -> str:
    """Return the only primary-key field of a model that a store keys by one value.

    Args:
        model: The model.
        why_single: Why the store holds one key value, completing the refusal
            message ("Mongo stores one key value in _id").

    Raises:
        ValueError: If the model declares no primary-key field.
        ConfigError: If the primary key is composite.
    """
    names = get_id_attributes(model)
    match names:
        case (name,):
            return name
        case _:
            raise ConfigError(
                f"{model.__qualname__} has the composite primary key ({', '.join(names)}); "
                f"{why_single}, so declare a single primary key"
            )


@dataclass(frozen=True, slots=True)
class PrimaryKey:
    """The primary-key fields of one model and the id form a repository takes for them.

    A single-column key is its value; a composite key is a tuple holding every
    key value in key order.

    Attributes:
        model_name: The model the key belongs to, named in refusals.
        names: The primary-key field names, in key order.
    """

    model_name: str
    names: tuple[str, ...]

    def values_of(self, obj_id: Any) -> tuple[Any, ...]:
        """Return the key values *obj_id* names, one per key field.

        Raises:
            ValueError: If the key is composite and *obj_id* is not a tuple of
                one value per key field.
        """
        if len(self.names) == 1:
            return (obj_id,)
        if isinstance(obj_id, tuple) and len(obj_id) == len(self.names):
            return obj_id
        raise ValueError(
            f"{self.model_name} has the composite primary key ({', '.join(self.names)}); "
            f"pass a tuple of {len(self.names)} values in that order, not {obj_id!r}"
        )

    def values_in(self, row: Any) -> tuple[Any, ...]:
        """Return the key values *row* holds, ``None`` for a field it lacks."""
        return tuple(getattr(row, name, None) for name in self.names)

    def id_of(self, row: Any) -> Any:
        """Return the id of *row* in the form :meth:`values_of` accepts."""
        values = self.values_in(row)
        return values[0] if len(values) == 1 else values
