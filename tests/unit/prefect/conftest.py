"""Fixtures shared by the Prefect unit tests."""

from __future__ import annotations

import textwrap
from collections.abc import Callable
from pathlib import Path

import pytest


@pytest.fixture
def write_yaml(tmp_path: Path) -> Callable[[str, str], Path]:
    """Write a YAML document under ``tmp_path`` and return its path."""

    def write(name: str, body: str) -> Path:
        path = tmp_path / name
        path.write_text(textwrap.dedent(body), encoding="utf-8")
        return path

    return write
