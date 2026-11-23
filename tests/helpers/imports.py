"""What a module imports at run time, read from its source or measured in a child interpreter."""

from __future__ import annotations

import ast
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import cast

_SRC = Path(__file__).resolve().parents[2] / "src"

_DELTA_SCRIPT = """
import importlib, json, sys
sys.path.insert(0, sys.argv[1])
baseline = frozenset(sys.modules)
importlib.import_module(sys.argv[2])
print(json.dumps(sorted(frozenset(sys.modules) - baseline)))
"""


@dataclass(frozen=True)
class ImportDelta:
    """The modules importing one module adds to an isolated interpreter."""

    modules: frozenset[str]

    @property
    def loom(self) -> frozenset[str]:
        """The loaded ``loom`` modules."""
        return frozenset(name for name in self.modules if name.split(".")[0] == "loom")

    @property
    def third_party(self) -> frozenset[str]:
        """Top-level packages loaded from outside the stdlib and loom."""
        roots = {name.split(".")[0] for name in self.modules}
        return frozenset(roots - sys.stdlib_module_names - {"loom"})

    def under(self, *roots: str) -> frozenset[str]:
        """The loaded modules that are one of *roots* or inside one."""
        return frozenset(
            name
            for name in self.modules
            if any(name == root or name.startswith(f"{root}.") for root in roots)
        )


def import_delta(module: str) -> ImportDelta:
    """What importing *module* loads in an isolated interpreter that sees ``src``."""
    result = subprocess.run(
        [sys.executable, "-I", "-c", _DELTA_SCRIPT, str(_SRC), module],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise AssertionError(result.stderr)
    return ImportDelta(frozenset(json.loads(result.stdout.splitlines()[-1])))


def _is_type_checking_block(node: ast.AST) -> bool:
    if not isinstance(node, ast.If):
        return False
    test = node.test
    name = test.attr if isinstance(test, ast.Attribute) else getattr(test, "id", None)
    return name == "TYPE_CHECKING"


def runtime_nodes(tree: ast.AST) -> list[ast.AST]:
    """Every node of *tree* but those under ``if TYPE_CHECKING:``, which never run."""
    nodes: list[ast.AST] = []
    pending = [tree]
    while pending:
        node = pending.pop()
        nodes.append(node)
        for child in ast.iter_child_nodes(node):
            if _is_type_checking_block(child):
                pending.extend(cast(ast.If, child).orelse)
            else:
                pending.append(child)
    return nodes


def imported_roots(tree: ast.AST) -> set[str]:
    """Top-level package of every absolute import that runs, function-local ones included."""
    return {module.split(".")[0] for module in imported_modules(tree, package="")}


def imported_modules(tree: ast.AST, *, package: str) -> set[str]:
    """Dotted name of every import that runs, relative ones resolved against *package*.

    A ``from`` import also yields each imported name under its module, so
    ``from loom import composition`` yields ``loom.composition``.
    """
    modules: set[str] = set()
    for node in runtime_nodes(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                base = node.module or ""
            elif package:
                parent = package.rsplit(".", node.level - 1)[0]
                base = f"{parent}.{node.module}" if node.module else parent
            else:
                continue
            modules.add(base)
            modules.update(f"{base}.{alias.name}" for alias in node.names)
    return modules
