"""SQLAlchemy IntegrityError classification and handling decorator.

Converts low-level database constraint violations into domain
:class:`~loom.core.errors.LoomError` subclasses so the application layer
never sees driver-specific exception types.

Supports two detection strategies in priority order:

1. **SQLSTATE code** — used by PostgreSQL via psycopg2 (``exc.orig.pgcode``),
   psycopg3, and asyncpg (``exc.orig.sqlstate``).  Most reliable; enriched
   with diagnostic metadata (constraint name, column name, detail message)
   when available.
2. **Message pattern matching** — used for SQLite and any other driver that
   does not expose a SQLSTATE.  Compiled regex patterns match the standard
   SQLite constraint violation messages.

Usage::

    from loom.core.repository.sqlalchemy.integrity import handle_integrity_errors

    class MyMixin:
        @handle_integrity_errors
        async def create(self, data: Struct) -> Output:
            ...
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar

from sqlalchemy.exc import IntegrityError

from loom.core.errors import Conflict, LoomError, SystemError

R = TypeVar("R")

# ---------------------------------------------------------------------------
# PostgreSQL SQLSTATE codes
# ---------------------------------------------------------------------------

_PG_FOREIGN_KEY = "23503"
_PG_UNIQUE = "23505"
_PG_NOT_NULL = "23502"
_PG_CHECK = "23514"


# ---------------------------------------------------------------------------
# SQLite pattern registry
# ---------------------------------------------------------------------------

_SqliteFactory = Callable[[re.Match[str]], LoomError]

_SQLITE_PATTERNS: list[tuple[re.Pattern[str], _SqliteFactory]] = [
    (
        re.compile(r"FOREIGN KEY constraint failed", re.IGNORECASE),
        lambda _: Conflict("A referenced entity does not exist."),
    ),
    (
        re.compile(r"UNIQUE constraint failed: (?P<cols>.+)", re.IGNORECASE),
        lambda m: Conflict(f"A record with these values already exists ({m.group('cols')})."),
    ),
    (
        re.compile(r"NOT NULL constraint failed: (?P<col>.+)", re.IGNORECASE),
        lambda m: Conflict(f"Field '{m.group('col')}' is required."),
    ),
    (
        re.compile(r"CHECK constraint failed: (?P<name>.+)", re.IGNORECASE),
        lambda m: Conflict(f"Value violates check constraint '{m.group('name')}'."),
    ),
]


# ---------------------------------------------------------------------------
# Driver helpers
# ---------------------------------------------------------------------------


def _pgcode(exc: IntegrityError) -> str | None:
    """Return the SQLSTATE code from the underlying DBAPI exception.

    Checks ``pgcode`` (psycopg2 / psycopg3) and ``sqlstate`` (asyncpg) in
    that order.  Returns ``None`` when neither attribute is present.
    """
    orig = exc.orig
    code = getattr(orig, "pgcode", None) or getattr(orig, "sqlstate", None)
    return str(code) if code else None


def _diag(exc: IntegrityError, attr: str) -> str:
    """Safely read a field from the psycopg2/psycopg3 ``diag`` object."""
    diag = getattr(exc.orig, "diag", None)
    return str(getattr(diag, attr, "") or "")


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def _from_pgcode(code: str, exc: IntegrityError) -> LoomError:
    constraint = _diag(exc, "constraint_name")
    column = _diag(exc, "column_name")
    detail = _diag(exc, "message_detail")

    if code == _PG_FOREIGN_KEY:
        suffix = f" {detail}".rstrip() if detail else ""
        return Conflict(f"A referenced entity does not exist.{suffix}")

    if code == _PG_UNIQUE:
        suffix = f" ({constraint})" if constraint else ""
        return Conflict(f"A record with these values already exists{suffix}.")

    if code == _PG_NOT_NULL:
        name = column or "unknown"
        return Conflict(f"Field '{name}' is required.")

    if code == _PG_CHECK:
        suffix = f" '{constraint}'" if constraint else ""
        return Conflict(f"Value violates check constraint{suffix}.")

    return SystemError(f"Database integrity violation (SQLSTATE {code})")


def _from_message(msg: str) -> LoomError:
    for pattern, factory in _SQLITE_PATTERNS:
        match = pattern.search(msg)
        if match:
            return factory(match)
    return SystemError("Database integrity violation")


def classify_integrity_error(exc: IntegrityError) -> LoomError:
    """Convert a SQLAlchemy ``IntegrityError`` to a domain :class:`~loom.core.errors.LoomError`.

    Tries SQLSTATE-based classification first (PostgreSQL / asyncpg), then
    falls back to message-pattern matching (SQLite and other drivers).

    Args:
        exc: The ``IntegrityError`` raised by SQLAlchemy.

    Returns:
        A :class:`~loom.core.errors.Conflict` for constraint violations or a
        :class:`~loom.core.errors.SystemError` for unrecognised violations.
    """
    code = _pgcode(exc)
    if code is not None:
        return _from_pgcode(code, exc)
    return _from_message(str(exc.orig))


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------


def handle_integrity_errors(
    method: Callable[..., Awaitable[R]],
) -> Callable[..., Awaitable[R]]:
    """Decorator that converts ``IntegrityError`` to a domain error.

    Apply to any async repository method that performs writes.  The
    underlying session scope already rolls back on exception; this decorator
    intercepts the propagated ``IntegrityError`` after rollback and converts
    it to a :class:`~loom.core.errors.Conflict` or
    :class:`~loom.core.errors.SystemError`.

    Example::

        @handle_integrity_errors
        async def create(self, data: Struct) -> Output:
            async with self._session_scope(write=True) as session:
                ...
    """

    @wraps(method)
    async def _wrapper(*args: Any, **kwargs: Any) -> R:
        try:
            return await method(*args, **kwargs)
        except IntegrityError as exc:
            raise classify_integrity_error(exc) from exc

    return _wrapper
