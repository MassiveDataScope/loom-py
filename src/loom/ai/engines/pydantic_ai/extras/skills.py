"""The skills harness (``ai-harness`` extra): pydantic-ai-harness's ``Skills`` capability."""

from __future__ import annotations

from collections.abc import Collection
from pathlib import Path

from pydantic_ai.workspaces import LocalWorkspaceBackend, Workspace
from pydantic_ai.workspaces.readonly import ReadOnlyWorkspace
from pydantic_ai_harness import Skills

__all__ = ["library_skills"]


def library_skills(directory: str | Path, names: Collection[str]) -> Skills:
    """Build ``Skills`` reading one library through a read-only workspace of its own.

    The harness reads a library from a workspace. The run's workspace would hand
    the model file tools over the whole working directory, so the library gets
    its own backend rooted at the directory the compiler resolved, read-only and
    never attached to the run: no tool of the run can reach it, and nothing in it
    can be written or executed.

    Args:
        directory: The skill library the compiler resolved; it stays inside the root.
        names: The exact skill names the artifact grants.

    Returns:
        The harness capability exposing those skills.
    """
    workspace = ReadOnlyWorkspace(Workspace(LocalWorkspaceBackend(directory)))
    return Skills(".", include=list(names), workspace=workspace)
