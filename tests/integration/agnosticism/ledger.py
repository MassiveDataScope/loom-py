"""Synthetic product 3: a bigint boundary that is itself a FK to a global table.

Exercises C9 (scoped -> global FK with RESTRICT), composite UNIQUE and index
declarations (C5), and a serial id inside a composite PK.
"""

from __future__ import annotations

import datetime as dt

from loom.core.model import BaseModel, ColumnField, OnDelete, Privilege, RowScoped, ScopedField
from loom.core.model.types import BigInteger, DateTime, Integer, Numeric, Text

SCHEMA = "ledger"
GROUPS = ("ledger_readers", "ledger_writers")
USERS = {"ledger_rw": "write", "ledger_ops": "bypass"}
SCOPE_BINDINGS = {"account": "identity.account"}


class Account(BaseModel):
    __tablename__ = "accounts"
    __privileges__ = {"readers": frozenset({Privilege.SELECT})}
    id: int = ColumnField(BigInteger, primary_key=True)
    label: str = ColumnField(Text)


class Entry(BaseModel, RowScoped):
    __tablename__ = "entries"
    __unique__ = (("account_id", "reference"),)
    __indexes__ = (("account_id", "booked_on"),)
    account_id: int = ScopedField(
        BigInteger,
        primary_key=True,
        scope="account",
        foreign_key="accounts.id",
        on_delete=OnDelete.RESTRICT,
    )
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    reference: str = ColumnField(Text)
    booked_on: dt.datetime = ColumnField(DateTime())
    amount: float = ColumnField(Numeric())


MODELS = (Account, Entry)
