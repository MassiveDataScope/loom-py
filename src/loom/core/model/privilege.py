"""The closed set of row privileges a product may grant on a table."""

from __future__ import annotations

from enum import StrEnum


class Privilege(StrEnum):
    """Row operations subject to row-level security.

    TRUNCATE, REFERENCES and TRIGGER are deliberately absent: none of them
    honours row-level security, so none of them can ever be granted to a
    database user that is not meant to bypass it.
    """

    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


READ_WRITE: frozenset[Privilege] = frozenset(Privilege)
