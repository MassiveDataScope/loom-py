"""Derive the id of a throwaway keyed model, for the tests of the formula."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, BusinessKey, ColumnField, KeyHash, LoomStructMeta


def derived_id(key_hash: KeyHash, **values: Any) -> str:
    """Return the id *key_hash* derives from *values*, as a model keyed by them would."""
    model = LoomStructMeta(
        "AcmeKeyed",
        (BaseModel,),
        {
            "__tablename__": "acme_keyed",
            "__annotations__": {"id": str, **{name: type(value) for name, value in values.items()}},
            "id": ColumnField(primary_key=True, derived=key_hash),
            "__business_key__": BusinessKey(*values),
        },
    )
    return model.key_for(values)
