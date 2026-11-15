"""Every ``literalinclude`` in the documentation names an example a docs test runs."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
EXAMPLES = ROOT / "tests" / "docs" / "examples"
_DIRECTIVE = re.compile(
    r"```\{literalinclude\}\s+(?P<path>\S+)\n(?P<options>(?::[^\n]*\n)*)", re.MULTILINE
)
_PYOBJECT = re.compile(r"^:pyobject:\s*(?P<name>\S+)\s*$", re.MULTILINE)


def _includes() -> list[tuple[Path, Path, str | None]]:
    found = []
    for page in sorted(DOCS.rglob("*.md")):
        for match in _DIRECTIVE.finditer(page.read_text(encoding="utf-8")):
            target = (page.parent / match["path"]).resolve()
            pyobject = _PYOBJECT.search(match["options"])
            found.append((page, target, pyobject["name"] if pyobject else None))
    return found


def _top_level_names(source: Path) -> set[str]:
    tree = ast.parse(source.read_text(encoding="utf-8"))
    return {
        node.name
        for node in tree.body
        if isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef)
    }


INCLUDES = _includes()


class TestLiteralIncludes:
    def test_the_guides_include_examples(self) -> None:
        assert INCLUDES

    @pytest.mark.parametrize(
        ("page", "target", "pyobject"),
        INCLUDES,
        ids=[f"{p.relative_to(DOCS)}:{t.name}:{o}" for p, t, o in INCLUDES],
    )
    def test_an_include_names_an_object_of_an_example(
        self, page: Path, target: Path, pyobject: str | None
    ) -> None:
        assert target.is_relative_to(EXAMPLES), f"{page} includes {target} outside the examples"
        assert target.is_file(), f"{page} includes {target}, which does not exist"
        if pyobject is not None:
            assert pyobject in _top_level_names(target)

    def test_every_example_module_is_imported_by_a_docs_test(self) -> None:
        tests = "".join(p.read_text(encoding="utf-8") for p in EXAMPLES.parent.glob("test_*.py"))
        modules = {p.stem for p in EXAMPLES.glob("*.py")} - {"__init__"}
        assert {m for m in modules if f"tests.docs.examples.{m} import" not in tests} == set()
