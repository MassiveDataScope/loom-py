"""Reading the JSON Schema a :class:`~loom.core.model.LoomType` publishes.

msgspec puts the root of a model behind ``{"$ref": "#/$defs/Name"}`` and
pydantic inlines it; both keep nested definitions under ``$defs`` and write
an optional value as an ``anyOf`` with a ``null`` branch.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

DEFS = "$defs"
REF = "$ref"
DEFS_PREFIX = "#/$defs/"

_NULL = {"type": "null"}


def definition(schema: Mapping[str, Any], definitions: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return the definition *schema* references, or *schema* when it references none."""
    ref = schema.get(REF)
    return definitions[ref.removeprefix(DEFS_PREFIX)] if ref is not None else schema


def without_null(prop: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return an optional property as its single non-null branch, without a ``None`` default.

    A property that is not ``X | None`` is returned as it is.
    """
    options = [option for option in prop.get("anyOf", ()) if option != _NULL]
    if len(options) != 1:
        return prop
    rest = {k: v for k, v in prop.items() if k != "anyOf" and not (k == "default" and v is None)}
    return {**options[0], **rest}
