"""The values a struct persists when it is created."""

from __future__ import annotations

from typing import Any, ClassVar

import msgspec


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
