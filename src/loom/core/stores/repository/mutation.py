from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class MutationEvent:
    """Immutable record of a single mutation within a transaction.

    Attributes:
        entity: Normalized entity name (e.g. ``"user"``).
        op: Operation type (``"create"``, ``"update"``, or ``"delete"``).
        ids: Primary keys of the affected entities.
        changed_fields: Names of fields that were modified (empty for deletes).
        tags: Cache dependency tags to invalidate upon commit.
    """

    entity: str
    op: str
    ids: tuple[object, ...]
    changed_fields: frozenset[str] = field(default_factory=frozenset)
    tags: frozenset[str] = field(default_factory=frozenset)
