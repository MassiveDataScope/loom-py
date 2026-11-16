from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

PackageWriter = Callable[[str, dict[str, str]], Path]


@pytest.fixture
def write_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[PackageWriter]:
    """Write a package of *files* under ``tmp_path``, importable until the test ends."""
    monkeypatch.syspath_prepend(str(tmp_path))
    written: list[str] = []

    def write(name: str, files: dict[str, str]) -> Path:
        written.append(name)
        root = tmp_path / name
        for relative, source in {"__init__.py": "", **files}.items():
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(source)
        return root

    yield write
    for name in tuple(sys.modules):
        if name.split(".")[0] in written:
            sys.modules.pop(name)
