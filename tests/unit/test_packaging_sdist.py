"""Contract tests for the source distribution of ``loom-kernel``."""

from __future__ import annotations

import shutil
import subprocess
import tarfile
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
EXCLUDED = ("tests/", "benchmarks/", "docs/", ".github/", "scripts/")
REQUIRED = (
    "src/loom/py.typed",
    "src/loom/ai/declarative/schemas/",
    "README.md",
    "LICENSE",
    "CHANGELOG.md",
    "pyproject.toml",
)

pytestmark = pytest.mark.skipif(shutil.which("uv") is None, reason="uv is not installed")


def _build(out_dir: Path, *args: str) -> Path:
    subprocess.run(
        ["uv", "build", "--quiet", "--out-dir", str(out_dir), *args],
        cwd=ROOT,
        check=True,
    )
    (built,) = [p for p in out_dir.iterdir() if p.suffix in {".gz", ".whl"}]
    return built


def _sdist_members(sdist: Path) -> list[str]:
    with tarfile.open(sdist) as archive:
        return [name.split("/", 1)[1] for name in archive.getnames() if "/" in name]


def _wheel_members(wheel: Path) -> set[str]:
    with zipfile.ZipFile(wheel) as archive:
        return {name for name in archive.namelist() if ".dist-info/" not in name}


@pytest.fixture(scope="module")
def sdist(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return _build(tmp_path_factory.mktemp("sdist"), "--sdist")


def test_sdist_holds_only_the_library_sources(sdist: Path) -> None:
    members = _sdist_members(sdist)

    assert not [m for m in members if m.startswith(EXCLUDED)]
    for required in REQUIRED:
        assert any(m == required or m.startswith(required) for m in members), required


def test_wheel_built_from_the_sdist_matches_the_wheel_built_from_the_repository(
    sdist: Path, tmp_path: Path
) -> None:
    from_repo = _build(tmp_path / "repo", "--wheel")
    from_sdist = _build(tmp_path / "sdist", "--wheel", str(sdist))

    assert _wheel_members(from_sdist) == _wheel_members(from_repo)
