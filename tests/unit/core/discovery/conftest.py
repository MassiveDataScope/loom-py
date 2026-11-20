from __future__ import annotations

import importlib
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.helpers.package_writer import DistributionWriter, PackageWriter


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


@pytest.fixture
def install_distribution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DistributionWriter:
    """Install a ``.dist-info`` whose entry points name modules of ``write_package``."""

    monkeypatch.syspath_prepend(str(tmp_path))

    def install(name: str, groups: dict[str, list[str]]) -> Path:
        dist_info = tmp_path / f"{name}-0.1.0.dist-info"
        dist_info.mkdir()
        (dist_info / "METADATA").write_text(
            f"Metadata-Version: 2.1\nName: {name}\nVersion: 0.1.0\n"
        )
        sections = (
            f"[{group}]\n" + "".join(f"{module} = {module}\n" for module in modules)
            for group, modules in groups.items()
        )
        (dist_info / "entry_points.txt").write_text("\n".join(sections))
        importlib.invalidate_caches()
        return dist_info

    return install
