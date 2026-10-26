"""What a module imports at run time, read from its source."""

from __future__ import annotations

import ast
from typing import cast


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
    """Dotted name of every import that runs, relative ones resolved against *package*."""
    modules: set[str] = set()
    for node in runtime_nodes(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                if node.module:
                    modules.add(node.module)
                continue
            if not package:
                continue
            base = package.split(".")[: len(package.split(".")) - node.level + 1]
            modules.add(".".join([*base, node.module] if node.module else base))
    return modules
