"""Flat include/exclude selection shared by every filtered capability.

``mcp``, ``skills`` and ``a2a`` all narrow a list of names the same way: an
``include`` allow-list of glob patterns, then an ``exclude`` deny-list applied
on top.  The rule lives here once so the compiler (which filters skill names
offline) and the host (which filters real tool names at start-up) cannot
drift apart.

Matching is case-sensitive on every platform: names come from a remote server
or a directory listing, and a filter that silently widened on a
case-insensitive filesystem would be a permission difference between
environments.
"""

from __future__ import annotations

from collections.abc import Sequence
from fnmatch import fnmatchcase


def matches(name: str, patterns: Sequence[str]) -> bool:
    """Report whether ``name`` matches any of the glob patterns.

    Args:
        name: Candidate name.
        patterns: Glob patterns, matched case-sensitively.

    Returns:
        ``True`` when at least one pattern matches; ``False`` for an empty
        pattern sequence.
    """
    return any(fnmatchcase(name, pattern) for pattern in patterns)


def admits(name: str, *, include: Sequence[str], exclude: Sequence[str]) -> bool:
    """Report whether ``name`` survives the include-then-exclude filter.

    The single-name counterpart of :func:`select_names`, for a caller that
    decides one name at a time instead of narrowing a whole list — the
    engine's own per-tool predicate and a host grant view both do.

    Args:
        name: Candidate name.
        include: Glob patterns to keep; empty means every name.
        exclude: Glob patterns to drop, applied after ``include``.

    Returns:
        ``True`` when ``name`` is not filtered out.
    """
    if include and not matches(name, include):
        return False
    return not matches(name, exclude)


def select_names(
    names: Sequence[str],
    *,
    include: Sequence[str],
    exclude: Sequence[str],
) -> tuple[str, ...]:
    """Narrow ``names`` through an include allow-list and an exclude deny-list.

    An empty ``include`` means "every name"; ``exclude`` is always applied
    afterwards, so it wins over ``include``.  The input order is preserved and
    repeated names are emitted once.

    Args:
        names: Candidate names, in the order the result should keep.
        include: Glob patterns to keep; empty means all.
        exclude: Glob patterns to drop, applied after ``include``.

    Returns:
        The selected names, de-duplicated and in input order.

    Example:
        >>> select_names(["read", "write", "admin"], include=["*"], exclude=["admin"])
        ('read', 'write')
    """
    selected: list[str] = []
    seen: set[str] = set()
    for name in names:
        if name in seen:
            continue
        if not admits(name, include=include, exclude=exclude):
            continue
        seen.add(name)
        selected.append(name)
    return tuple(selected)
