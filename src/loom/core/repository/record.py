"""The values a struct persists when it is created."""

from __future__ import annotations

from typing import Any, ClassVar

import msgspec


class RecordLayout:
    """The fields of one struct type that can hold ``msgspec.UNSET``.

    A layout is built once per struct type; :meth:`of` returns the same
    object for the same type.
    """

    _layouts: ClassVar[dict[type[msgspec.Struct], RecordLayout]] = {}

    def __init__(self, struct_type: type[msgspec.Struct]) -> None:
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
    def of(cls, struct_type: type[msgspec.Struct]) -> RecordLayout:
        """Return the layout of *struct_type*, building it on first use."""
        layout = cls._layouts.get(struct_type)
        if layout is None:
            layout = cls._layouts[struct_type] = cls(struct_type)
        return layout

    def values(self, row: msgspec.Struct) -> dict[str, Any]:
        """Return the field values of *row* keyed by field name, ``UNSET`` fields left out.

        A field left at its default is included, whatever ``omit_defaults``
        the struct declares. Values are not converted.
        """
        values = msgspec.structs.asdict(row)
        for name in self._unsettable:
            if values[name] is msgspec.UNSET:
                del values[name]
        return values
