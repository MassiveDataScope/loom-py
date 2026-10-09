"""Containment of relative paths inside the directory they are anchored to."""

from __future__ import annotations

from pathlib import Path, PurePath

__all__ = ["escapes", "is_within"]


def escapes(relative: str) -> bool:
    """Report whether *relative* could name something outside its anchor.

    Args:
        relative: Path or glob written relative to an anchor directory.

    Returns:
        ``True`` when it is absolute or climbs with a ``..`` segment.
    """
    path = PurePath(relative)
    return path.is_absolute() or ".." in path.parts


def is_within(path: Path, root: Path) -> bool:
    """Report whether *path*, symlinks resolved, stays inside *root*.

    Args:
        path: Path found on disk.
        root: Directory *path* must not leave.

    Returns:
        Whether the resolved *path* is *root* or below it.
    """
    return path.resolve().is_relative_to(root.resolve())
