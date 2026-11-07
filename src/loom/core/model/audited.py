"""Marker and descriptor for tables whose every row change is recorded in the audit log."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import ClassVar

from loom.core.model.redaction import is_secret_name


class Audited:
    """Mark a model as audited; adds no column.

    Every insert, delete and value-changing update of the table is recorded
    in the store's audit log by a trigger the guard installs. Columns named
    in ``__audit_exclude__``, and every column whose name is a credential
    (:func:`~loom.core.model.redaction.is_secret_name`), are never written
    to the log: a change to one is recorded as redacted. The table needs a
    primary key, and no excluded or secret column may be part of it.
    """

    __audited__: ClassVar[bool] = True
    __audit_exclude__: ClassVar[frozenset[str]] = frozenset()


@dataclass(frozen=True, slots=True)
class AuditedTable:
    """A compiled audited table: its key columns and the columns never written to the log."""

    schema: str | None
    name: str
    key_columns: tuple[str, ...]
    excluded: frozenset[str]


AuditedTables = Mapping[tuple[str | None, str], AuditedTable]  # by schema and name


def is_audited(model: type) -> bool:
    """Whether *model* carries the :class:`Audited` marker."""
    return bool(getattr(model, "__audited__", False))


def audited_table(
    model: type,
    *,
    schema: str | None,
    name: str,
    columns: Iterable[str],
    key_columns: Iterable[str],
) -> AuditedTable:
    """Compile *model*'s audit declaration.

    Raises:
        ValueError: When an excluded column does not exist, or an excluded or
            secret column is part of the primary key; the message names the
            table and the column.
    """
    names = tuple(columns)
    keys = tuple(key_columns)
    declared = frozenset(getattr(model, "__audit_exclude__", frozenset()))
    unknown = sorted(declared - set(names))
    if unknown:
        raise ValueError(f"audited table {name} excludes unknown columns {unknown}")
    excluded = declared | {column for column in names if is_secret_name(column)}
    in_key = sorted(excluded & set(keys))
    if in_key:
        raise ValueError(
            f"audited table {name}: {in_key} cannot be excluded from the log "
            "and be part of the primary key"
        )
    return AuditedTable(schema=schema, name=name, key_columns=keys, excluded=excluded)
