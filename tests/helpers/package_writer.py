"""The type of the ``write_package`` fixture."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

PackageWriter = Callable[[str, dict[str, str]], Path]
"""Writes the package named by the first argument with the files of the second; returns its root."""
