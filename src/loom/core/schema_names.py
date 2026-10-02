"""Names around one application schema and the identifier rules Postgres imposes on them.

This module depends on the standard library only, so ``loom schema init`` can
propose a configuration without the database extras installed.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

MAX_IDENTIFIER_LENGTH: Final = 63
MAX_SCHEMA_LENGTH: Final = 47
NAMING_CONVENTION_KINDS: Final = frozenset({"pk", "fk", "uq", "ck", "ix"})
POSTGRES_RESERVED_WORDS: Final = frozenset(
    {
        "all",
        "analyse",
        "analyze",
        "and",
        "any",
        "array",
        "as",
        "asc",
        "asymmetric",
        "authorization",
        "between",
        "binary",
        "both",
        "case",
        "cast",
        "check",
        "collate",
        "collation",
        "column",
        "concurrently",
        "constraint",
        "create",
        "cross",
        "current_catalog",
        "current_date",
        "current_role",
        "current_schema",
        "current_time",
        "current_timestamp",
        "current_user",
        "default",
        "deferrable",
        "desc",
        "distinct",
        "do",
        "else",
        "end",
        "except",
        "false",
        "fetch",
        "for",
        "foreign",
        "freeze",
        "from",
        "full",
        "grant",
        "group",
        "having",
        "ilike",
        "in",
        "initially",
        "inner",
        "intersect",
        "into",
        "is",
        "isnull",
        "join",
        "lateral",
        "leading",
        "left",
        "like",
        "limit",
        "localtime",
        "localtimestamp",
        "natural",
        "new",
        "not",
        "notnull",
        "null",
        "of",
        "off",
        "offset",
        "old",
        "on",
        "only",
        "or",
        "order",
        "outer",
        "over",
        "overlaps",
        "placing",
        "primary",
        "references",
        "returning",
        "right",
        "select",
        "session_user",
        "similar",
        "some",
        "symmetric",
        "system_user",
        "table",
        "tablesample",
        "then",
        "to",
        "trailing",
        "true",
        "union",
        "unique",
        "user",
        "using",
        "variadic",
        "verbose",
        "when",
        "where",
        "window",
        "with",
    }
)
_IDENTIFIER: Final = re.compile(r"[a-z_][a-z0-9_]*")
_SPECIAL_ROLES: Final = frozenset(
    {"public", "none", "current_role", "current_user", "session_user"}
)


def sql_identifier(name: str, *, max_length: int = MAX_IDENTIFIER_LENGTH) -> str:
    """Return ``name`` when Postgres stores it exactly as written and resolves it to itself.

    Lowercase letters, digits and ``_``; at most ``max_length`` characters so
    Postgres never truncates it; not a reserved word, a special role name or a
    ``pg_`` name.

    Raises:
        ValueError: Naming the offending identifier.
    """
    if (
        not _IDENTIFIER.fullmatch(name)
        or len(name) > max_length
        or name in POSTGRES_RESERVED_WORDS
        or name in _SPECIAL_ROLES
        or name.startswith("pg_")
    ):
        raise ValueError(
            f"{name!r} is not a usable SQL identifier: lowercase letters, digits and '_', "
            f"at most {max_length} characters, not a reserved word, special role or pg_ name"
        )
    return name


def schema_identifier(name: str) -> str:
    """Validate a scoped schema name; short enough that every guard object name fits."""
    return sql_identifier(name, max_length=MAX_SCHEMA_LENGTH)


def naming_convention(value: Mapping[str, str] | None) -> dict[str, str] | None:
    """Validate a SQLAlchemy ``naming_convention`` keyed by constraint kind.

    ``None`` keeps SQLAlchemy's default. The runtime and the migration paths
    both read ``database.schema.naming_convention`` through this function, so
    they compile the same names.

    Raises:
        ValueError: Naming the first key that is not ``pk``, ``fk``, ``uq``, ``ck`` or ``ix``.
    """
    if value is None:
        return None
    unknown = sorted(set(value) - NAMING_CONVENTION_KINDS)
    if unknown:
        raise ValueError(
            f"unknown kind {unknown[0]!r}; expected one of "
            f"{', '.join(sorted(NAMING_CONVENTION_KINDS))}"
        )
    return dict(value)


@dataclass(frozen=True, slots=True)
class SchemaNames:
    """The names around one application schema; declared by the product, never defaulted.

    ``loom schema init`` proposes them with :meth:`derived` and writes them into
    the product's configuration, where the product may change any of them.
    """

    guard: str
    readers: str
    writers: str
    version_table: str
    data_version_table: str

    @classmethod
    def derived(cls, schema: str) -> SchemaNames:
        """Propose the conventional names for ``schema``; used only to write the configuration."""
        schema_identifier(schema)
        return cls(
            guard=f"loom_guard_{schema}",
            readers=f"{schema}_readers",
            writers=f"{schema}_writers",
            version_table="alembic_version",
            data_version_table="alembic_version_data",
        )
