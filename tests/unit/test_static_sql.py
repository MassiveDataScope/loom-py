from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src" / "loom" / "core"
SCOPED_PACKAGES = (
    SRC / "repository" / "sqlalchemy" / "rls",
    SRC / "repository" / "sqlalchemy" / "migrations",
)
SCOPED_MODULES = (
    SRC / "backend" / "scoped_ddl.py",
    SRC / "repository" / "sqlalchemy" / "backend.py",
    SRC / "repository" / "sqlalchemy" / "session_settings.py",
    SRC / "locator.py",
)
SQL_SINK_FUNCTIONS = frozenset({"text", "DDL"})
SQL_SINK_METHODS = frozenset({"exec_driver_sql", "text"})
PLACEHOLDER = re.compile(r"\{[A-Za-z_][A-Za-z0-9_]*\}")


def _modules() -> list[Path]:
    found = [path for package in SCOPED_PACKAGES for path in sorted(package.rglob("*.py"))]
    return found + [path for path in SCOPED_MODULES if path.exists()]


def _module_constants(tree: ast.Module) -> frozenset[str]:
    return _local_constants(tree) | _imported_constants(tree)


def _imported_constants(tree: ast.Module) -> frozenset[str]:
    names: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or not (node.module or "").startswith("loom."):
            continue
        source = SRC.parents[1] / Path(*node.module.split("."))
        path = (
            source.with_suffix(".py")
            if source.with_suffix(".py").exists()
            else source / "__init__.py"
        )
        if not path.exists():
            continue
        exported = _local_constants(ast.parse(path.read_text(encoding="utf-8")))
        names.update(alias.asname or alias.name for alias in node.names if alias.name in exported)
    return frozenset(names)


def _local_constants(tree: ast.Module) -> frozenset[str]:
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Assign) and _is_literal(node.value):
            names.update(target.id for target in node.targets if isinstance(target, ast.Name))
        if (
            isinstance(node, ast.AnnAssign)
            and node.value is not None
            and _is_literal(node.value)
            and isinstance(node.target, ast.Name)
        ):
            names.add(node.target.id)
    return frozenset(names)


def _is_literal(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, str)


def _is_sql_sink(call: ast.Call) -> bool:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id in SQL_SINK_FUNCTIONS
    if isinstance(func, ast.Attribute):
        if func.attr in SQL_SINK_METHODS:
            return True
        return func.attr == "execute" and isinstance(func.value, ast.Name) and func.value.id == "op"
    return False


def _violations(path: Path) -> list[int]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    constants = _module_constants(tree)
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and _is_sql_sink(node)
        and node.args
        and not _is_literal(node.args[0])
        and not (isinstance(node.args[0], ast.Name) and node.args[0].id in constants)
    ]


def test_sql_reaching_the_database_is_a_literal_or_a_module_constant() -> None:
    violations = [
        f"{path.relative_to(SRC)}:{line}" for path in _modules() for line in _violations(path)
    ]

    assert violations == []


@pytest.mark.parametrize(
    "source",
    [
        'text(f"SELECT {x}")',
        'text("SELECT " + x)',
        'text("".join(parts))',
        'text("SELECT {}".format(x))',
        "text(query)",
        "DDL(statement)",
        'connection.exec_driver_sql(f"SET ROLE {owner}")',
        "op.execute(sql)",
    ],
)
def test_dynamic_sql_is_rejected(source: str, tmp_path: Path) -> None:
    module = tmp_path / "module.py"
    signature = "def run(x, parts, query, statement, owner, sql, connection, op):"
    module.write_text(f"{signature}\n    {source}\n")

    assert _violations(module) != []


def test_module_constants_are_accepted(tmp_path: Path) -> None:
    module = tmp_path / "module.py"
    module.write_text(
        'PROTECT = "SELECT protect(:tbl)"\n\ndef run():\n    text(PROTECT)\n    text("SELECT 1")\n'
    )

    assert _violations(module) == []


def test_packaged_sql_has_no_placeholders() -> None:
    offenders = [
        str(path.relative_to(SRC))
        for path in sorted(SRC.rglob("*.sql"))
        if PLACEHOLDER.search(path.read_text(encoding="utf-8"))
    ]

    assert offenders == []
