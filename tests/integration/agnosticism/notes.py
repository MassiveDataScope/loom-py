"""Synthetic product 1: rows scoped by a text owner key, plus an elevable editor.

Shares no vocabulary with any real product. The owner scope binds to the
identity subject; the editor scope is write-only and elevable.
"""

from __future__ import annotations

import datetime as dt

from loom.core.model import BaseModel, ColumnField, OnDelete, Privilege, RowScoped, ScopedField
from loom.core.model.types import DateTime, Integer, String, Text

SCHEMA = "notes"
GROUPS = ("notes_readers", "notes_writers")
USERS = {"notes_rw": "write", "notes_ro": "read", "notes_ops": "bypass"}
SCOPE_BINDINGS = {"owner": "identity.subject", "editor": "request.editor"}


class Note(BaseModel, RowScoped):
    __tablename__ = "notes"
    owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    editor: str = ScopedField(Text, scope="editor", on="write", elevable=True)
    body: str = ColumnField(Text)


class NoteItem(BaseModel, RowScoped):
    __tablename__ = "note_items"
    owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    note_id: int = ColumnField(Integer, foreign_key="notes.id", on_delete=OnDelete.CASCADE)
    label: str = ColumnField(Text)


class NoteEvent(BaseModel, RowScoped):
    __tablename__ = "note_events"
    __scope_privileges__ = frozenset({Privilege.SELECT})
    __partition_by__ = ("RANGE", "at")
    owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
    at: dt.datetime = ColumnField(DateTime(), primary_key=True)
    kind: str = ColumnField(Text)


MODELS = (Note, NoteItem, NoteEvent)
