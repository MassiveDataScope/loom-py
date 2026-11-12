"""Type checkers see inherited ``frozen`` and StrictStruct's keyword-only fields.

msgspec inherits ``frozen`` at run time, but without loom's own
``dataclass_transform`` ty refuses every subclass of ``LoomFrozenStruct`` that
does not repeat ``frozen=True``. This is a typing promise, so it is checked by
running each checker over a fixture: the fixture must produce exactly the
expected diagnostics, on the expected lines, and no others.
"""

from __future__ import annotations

import importlib.util
import re
import shutil
import subprocess
import sys
import textwrap
from collections.abc import Callable
from pathlib import Path

import pytest

_FIXTURE = textwrap.dedent("""
    import msgspec
    from loom.core.model import LoomFrozenStruct, StrictStruct

    class Bare(LoomFrozenStruct):
        x: int

    class Repeated(LoomFrozenStruct, frozen=True):
        x: int

    class Contract(StrictStruct):
        x: int
        y: str = msgspec.field(default="a")

    class Reopened(StrictStruct, forbid_unknown_fields=False):
        x: int

    Bare(x=1)
    Repeated(x=1)
    Contract(x=1, y="b")
    Reopened(x=1)
    bare = Bare(x=1)
    bare.x = 2  # EXPECT
    contract = Contract(x=1)
    contract.x = 3  # EXPECT
    Contract(1)  # EXPECT
""")

_EXPECTED_LINES = frozenset(
    number
    for number, line in enumerate(_FIXTURE.splitlines(), start=1)
    if line.endswith("# EXPECT")
)


def _tool(name: str) -> str:
    """The checker next to this interpreter, else on PATH; skip when absent.

    Lean CI legs (``--no-default-groups``) install no type checker.
    """
    found = shutil.which(str(Path(sys.executable).with_name(name))) or shutil.which(name)
    if found is None:
        pytest.skip(f"{name} is not installed in this environment")
    return found


def _pyright(module: Path, tmp_path: Path) -> list[str]:
    del tmp_path
    return [_tool("pyright"), "--pythonpath", sys.executable, str(module)]


def _mypy(module: Path, tmp_path: Path) -> list[str]:
    if importlib.util.find_spec("mypy") is None:
        pytest.skip("mypy is not installed in this environment")
    # Its own cache: the shared .mypy_cache may be mid-write by another run.
    return [sys.executable, "-m", "mypy", "--cache-dir", str(tmp_path / ".mypy"), str(module)]


def _ty(module: Path, tmp_path: Path) -> list[str]:
    del tmp_path
    concise = ["--output-format", "concise"]
    return [_tool("ty"), "check", "--python", sys.executable, *concise, str(module)]


@pytest.mark.slow
@pytest.mark.parametrize("command", [_pyright, _mypy, _ty], ids=["pyright", "mypy", "ty"])
def test_checkers_report_only_the_expected_lines(
    command: Callable[[Path, Path], list[str]], tmp_path: Path
) -> None:
    module = tmp_path / "fixture.py"
    module.write_text(_FIXTURE, encoding="utf-8")

    result = subprocess.run(  # noqa: S603
        command(module, tmp_path), capture_output=True, text=True, check=False, cwd=tmp_path
    )

    output = result.stdout + result.stderr
    reported = {int(n) for n in re.findall(r"fixture\.py:(\d+)", output)}
    assert reported == _EXPECTED_LINES, output
