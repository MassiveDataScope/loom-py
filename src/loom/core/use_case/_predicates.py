"""Shared predicate helpers for compute and rule pipeline steps.

Private module — not part of the public API.  Used internally by
:mod:`loom.core.use_case.compute` and :mod:`loom.core.use_case.rule`
to avoid duplicating the field-resolution and presence-check logic.
"""

from __future__ import annotations

from typing import Any

from loom.core.command.base import Command
from loom.core.command.introspection import get_patch_fields
from loom.core.errors import InvalidValue
from loom.core.use_case.field_ref import FieldExpr, FieldRef, PredicateOp


def resolve_from_command(command: Command, ref: FieldRef) -> Any:
    """Resolve a field value by traversing a :class:`FieldRef` path."""
    current: Any = command
    for item in ref.path:
        current = getattr(current, item)
    return current


def is_present(command: Command, fields_set: frozenset[str], ref: FieldRef) -> bool:
    """Return ``True`` when the field referenced by ``ref`` has a value.

    For patch commands, presence is determined by ``fields_set`` (the set of
    explicitly provided fields).  For all other cases, the field is considered
    present when its resolved value is not ``None``.
    """
    if (
        isinstance(ref.root, type)
        and issubclass(ref.root, Command)
        and ref.leaf in get_patch_fields(ref.root)
    ):
        return ref.leaf in fields_set
    return resolve_from_command(command, ref) is not None


def predicate_is_present(
    command: Command,
    fields_set: frozenset[str],
    predicate: FieldRef | FieldExpr,
) -> bool:
    """Evaluate a predicate (single ref or boolean expression) for presence.

    Supports ``FieldRef``, and compound expressions built with ``|`` (OR)
    and ``&`` (AND) via :class:`~loom.core.use_case.field_ref.FieldExpr`.
    """
    if isinstance(predicate, FieldRef):
        return is_present(command, fields_set, predicate)
    if predicate.op is PredicateOp.OR:
        return predicate_is_present(command, fields_set, predicate.left) or predicate_is_present(
            command, fields_set, predicate.right
        )
    if predicate.op is PredicateOp.AND:
        return predicate_is_present(command, fields_set, predicate.left) and predicate_is_present(
            command, fields_set, predicate.right
        )
    raise InvalidValue("Unsupported predicate op")
