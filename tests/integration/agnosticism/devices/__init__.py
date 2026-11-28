"""Synthetic product 5: rows scoped by an owner, holding a binary value and a network address."""

from __future__ import annotations

import msgspec

from loom.core.model import BaseModel, Bytes, ColumnField, Postgres, RowScoped, ScopedField
from loom.core.model.types import Integer, String

SCHEMA = "devices"
SCOPE_BINDINGS = {"owner": "identity.subject"}


class Device(BaseModel, RowScoped):
    __tablename__ = "devices"
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
    fingerprint: bytes = ColumnField(Bytes)
    address: str = ColumnField(Postgres.INET)


class RegisterDevice(msgspec.Struct, kw_only=True):
    owner_id: str
    fingerprint: bytes
    address: str


MODELS = (Device,)
