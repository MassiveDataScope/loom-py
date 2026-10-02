from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src"
LOOM = SRC / "loom"
SQLALCHEMY = LOOM / "core" / "repository" / "sqlalchemy"
SCANNED_PACKAGES = (SQLALCHEMY / "rls", SQLALCHEMY / "migrations", LOOM / "cli")
SCANNED_MODULES = (
    LOOM / "core" / "backend" / "scoped_ddl.py",
    LOOM / "core" / "backend" / "sqlalchemy.py",
    LOOM / "core" / "locator.py",
    LOOM / "core" / "schema_names.py",
    SQLALCHEMY / "__init__.py",
    SQLALCHEMY / "backend.py",
    SQLALCHEMY / "session_manager.py",
    SQLALCHEMY / "session_settings.py",
)
BOOTSTRAP = SQLALCHEMY / "rls" / "bootstrap.py"
SINKS = frozenset(
    {
        "execute",
        "executemany",
        "fetch",
        "fetchrow",
        "fetchval",
        "exec_driver_sql",
        "text",
        "DDL",
        "CheckConstraint",
    }
)
CLAUSE_SINKS = frozenset({"text", "DDL"})
SQL_KEYWORDS = frozenset({"statement", "text", "query", "sql", "sqltext", "clause"})
PINNED_LOADERS = frozenset({"revision.sql()", "preflight_sql()"})
PLACEHOLDER = re.compile(r"\{[A-Za-z_]\w*\}")
# The only SQL loom takes from outside its own source: the DDL fragments a product
# declares on its models (``__checks__`` expressions, ``__partial_unique__``
# predicates). They are static class attributes, product code with the trust of a
# hand-written Alembic revision, and never carry a runtime value. Each entry names
# the module, the one function that consumes the fragment and the exact call, so the
# same call anywhere else, or any other call in these functions, is still a violation.
DECLARED_DDL_FRAGMENTS = frozenset(
    {
        ("loom/core/backend/sqlalchemy.py", "_partial_unique_index", "text(partial.where)"),
        (
            "loom/core/backend/sqlalchemy.py",
            "_check_constraint",
            "CheckConstraint(expression, name=rule, info={_RULE_KEY: ('__checks__', rule)})",
        ),
    }
)


def _modules() -> list[Path]:
    found = [path for package in SCANNED_PACKAGES for path in sorted(package.rglob("*.py"))]
    return found + list(SCANNED_MODULES)


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _is_final(annotation: ast.expr) -> bool:
    target = annotation.value if isinstance(annotation, ast.Subscript) else annotation
    if isinstance(target, ast.Name):
        return target.id == "Final"
    return isinstance(target, ast.Attribute) and target.attr == "Final"


def _binding_targets(node: ast.stmt) -> list[ast.expr]:
    if isinstance(node, ast.Assign):
        return list(node.targets)
    if isinstance(node, (ast.AnnAssign, ast.AugAssign, ast.For, ast.AsyncFor)):
        return [node.target]
    if isinstance(node, (ast.With, ast.AsyncWith)):
        return [item.optional_vars for item in node.items if item.optional_vars is not None]
    return []


def _module_bindings(tree: ast.Module) -> dict[str, int]:
    names = [
        name.id
        for node in tree.body
        for target in _binding_targets(node)
        for name in ast.walk(target)
        if isinstance(name, ast.Name)
    ]
    declared = (node for node in ast.walk(tree) if isinstance(node, ast.Global))
    names += [name for node in declared for name in node.names]
    return {name: names.count(name) for name in set(names)}


def _sink_aliases(tree: ast.Module) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            aliases.update(
                {
                    alias.asname: alias.name
                    for alias in node.names
                    if alias.asname and alias.name in SINKS
                }
            )
        elif isinstance(node, ast.Assign) and isinstance(node.value, ast.Name):
            sink = aliases.get(node.value.id, node.value.id if node.value.id in SINKS else None)
            if sink is not None:
                aliases.update({t.id: sink for t in node.targets if isinstance(t, ast.Name)})
    return aliases


def _sql_arguments(call: ast.Call) -> list[ast.expr]:
    arguments = list(call.args[:1])
    arguments += [kw.value for kw in call.keywords if kw.arg is None or kw.arg in SQL_KEYWORDS]
    return arguments


def _source(root: Path, module: str) -> Path | None:
    base = root / Path(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.exists():
            return candidate
    return None


class _Module:
    def __init__(self, tree: ast.Module, root: Path, cache: dict[Path, frozenset[str]]) -> None:
        self.aliases = _sink_aliases(tree)
        self.finals: set[str] = set(_imported_finals(tree, root, cache))
        bindings = _module_bindings(tree)
        for node in tree.body:
            if self._is_final_sql(node, bindings):
                self.finals.add(node.target.id)

    def sink(self, call: ast.Call) -> str | None:
        func = call.func
        if isinstance(func, ast.Name):
            return self.aliases.get(func.id, func.id if func.id in SINKS else None)
        if isinstance(func, ast.Attribute) and func.attr in SINKS:
            return func.attr
        return None

    def accepts(self, node: ast.expr) -> bool:
        if isinstance(node, ast.Name):
            return node.id in self.finals
        if ast.unparse(node) in PINNED_LOADERS:
            return True
        if not isinstance(node, ast.Call) or self.sink(node) not in CLAUSE_SINKS:
            return False
        arguments = _sql_arguments(node)
        return (
            len(arguments) == 1
            and isinstance(arguments[0], ast.Name)
            and self.accepts(arguments[0])
        )

    def _is_final_sql(self, node: ast.stmt, bindings: dict[str, int]) -> bool:
        if not (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and _is_final(node.annotation)
            and bindings.get(node.target.id) == 1
            and node.value is not None
        ):
            return False
        value = node.value
        if isinstance(value, ast.Constant):
            return isinstance(value.value, str)
        return isinstance(value, ast.Call) and self.accepts(value)


def _imported_finals(
    tree: ast.Module, root: Path, cache: dict[Path, frozenset[str]]
) -> frozenset[str]:
    names: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or not (node.module or "").startswith("loom."):
            continue
        source = _source(root, node.module or "")
        if source is None:
            continue
        exported = _finals(source, root, cache)
        names.update(alias.asname or alias.name for alias in node.names if alias.name in exported)
    return frozenset(names)


def _finals(path: Path, root: Path, cache: dict[Path, frozenset[str]]) -> frozenset[str]:
    if path not in cache:
        cache[path] = frozenset()
        cache[path] = frozenset(_Module(_parse(path), root, cache).finals)
    return cache[path]


def _sink_calls(path: Path, root: Path = SRC) -> Iterator[tuple[_Module, ast.Call]]:
    tree = _parse(path)
    module = _Module(tree, root, {})
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and module.sink(node) is not None:
            yield module, node


def _enclosing_functions(path: Path) -> dict[tuple[int, int], str]:
    """The innermost function around each call of ``path``, keyed by the call's position.

    ``ast.walk`` visits outer functions before the functions they contain, so
    the last assignment for a call is its innermost function.
    """
    enclosing: dict[tuple[int, int], str] = {}
    for function in ast.walk(_parse(path)):
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for call in ast.walk(function):
                if isinstance(call, ast.Call):
                    enclosing[(call.lineno, call.col_offset)] = function.name
    return enclosing


def _is_declared_fragment(path: Path, root: Path, function: str, call: ast.Call) -> bool:
    if not path.is_relative_to(root):
        return False
    entry = (path.relative_to(root).as_posix(), function, ast.unparse(call))
    return entry in DECLARED_DDL_FRAGMENTS


def _violations(path: Path, root: Path = SRC) -> list[str]:
    violations: list[str] = []
    functions = _enclosing_functions(path)
    for module, call in _sink_calls(path, root):
        function = functions.get((call.lineno, call.col_offset), "")
        if _is_declared_fragment(path, root, function, call):
            continue
        arguments = _sql_arguments(call)
        starred = any(isinstance(argument, ast.Starred) for argument in call.args)
        if starred or not arguments or not all(module.accepts(a) for a in arguments):
            violations.append(f"{call.lineno}: {ast.unparse(call)}")
    return violations


def test_every_scanned_module_exists() -> None:
    assert [path for path in SCANNED_MODULES if not path.exists()] == []


def test_the_scan_reaches_the_bootstrap_driver_calls() -> None:
    receivers = {
        ast.unparse(call.func)
        for _, call in _sink_calls(BOOTSTRAP)
        if isinstance(call.func, ast.Attribute)
    }

    assert BOOTSTRAP in _modules()
    assert {"driver.execute", "driver.fetch"} <= receivers


def test_sql_reaching_the_database_is_a_final_constant_or_a_pinned_loader() -> None:
    violations = [
        f"{path.relative_to(SRC)}:{violation}"
        for path in _modules()
        for violation in _violations(path)
    ]

    assert violations == []


HEADER = """\
from typing import Final

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy import text as raw_text

QUERY: Final = "SELECT 1"
CLAUSE: Final = text(QUERY)
NOT_FINAL = "SELECT 1"
REASSIGNED: Final = "SELECT 1"
REASSIGNED = "SELECT 2"
run_sql = text
"""


def _module(tmp_path: Path, body: str) -> Path:
    module = tmp_path / "module.py"
    signature = "def run(x, parts, query, owner, sql, connection, conn, op, driver, revision):"
    module.write_text(f"{HEADER}\n\n{signature}\n    {body}\n", encoding="utf-8")
    return module


@pytest.mark.parametrize(
    "body",
    [
        'text(f"SELECT {x}")',
        'text("SELECT " + x)',
        'text("".join(parts))',
        'text("SELECT {}".format(x))',
        "text(query)",
        'text("SELECT 1")',
        "text(text=query)",
        "raw_text(query)",
        "run_sql(query)",
        'sa.DDL(f"CREATE TABLE {x} (id int)")',
        "sa.text(query)",
        'connection.exec_driver_sql(f"SET ROLE {owner}")',
        "op.execute(sql)",
        "conn.execute(query)",
        'conn.execute("SELECT 1")',
        "conn.execute(statement=query)",
        "conn.execute(QUERY, statement=query)",
        "conn.execute(*parts)",
        "conn.execute()",
        "conn.execute(NOT_FINAL)",
        "conn.execute(REASSIGNED)",
        "conn.execute(text(text(QUERY)))",
        "driver.executemany(query, parts)",
        "driver.fetch(query)",
        "driver.fetchrow(query)",
        "driver.fetchval(query)",
        "driver.execute(revision.sql(x))",
        "driver.execute(other.sql())",
        "sa.CheckConstraint(x)",
        'sa.CheckConstraint(f"{x} > 0", name="positive")',
        "sa.CheckConstraint(sqltext=x)",
    ],
)
def test_dynamic_sql_is_rejected(body: str, tmp_path: Path) -> None:
    assert _violations(_module(tmp_path, body)) != []


@pytest.mark.parametrize(
    "body",
    [
        "conn.execute(QUERY)",
        "conn.execute(QUERY, {'a': x})",
        "conn.execute(statement=QUERY, parameters=x)",
        "conn.execute(CLAUSE)",
        "conn.execute(text(QUERY))",
        "conn.execute(raw_text(QUERY))",
        "op.execute(sa.DDL(QUERY))",
        "driver.fetchval(QUERY, x)",
        "driver.execute(revision.sql())",
        "driver.execute(preflight_sql())",
        'sa.CheckConstraint(QUERY, name="positive")',
    ],
)
def test_final_constants_and_pinned_loaders_are_accepted(body: str, tmp_path: Path) -> None:
    assert _violations(_module(tmp_path, body)) == []


def test_final_constants_imported_from_a_loom_module_are_accepted(tmp_path: Path) -> None:
    package = tmp_path / "loom"
    package.mkdir()
    (package / "statements.py").write_text(
        'from typing import Final\n\nQUERY: Final = "SELECT 1"\nPLAIN = "SELECT 1"\n',
        encoding="utf-8",
    )
    module = tmp_path / "module.py"
    module.write_text(
        "from loom.statements import PLAIN, QUERY as STATEMENT\n\n"
        "def run(conn):\n    conn.execute(STATEMENT)\n    conn.execute(PLAIN)\n",
        encoding="utf-8",
    )

    assert _violations(module, root=tmp_path) == ["5: conn.execute(PLAIN)"]


def test_packaged_sql_has_no_placeholders() -> None:
    offenders = [
        str(path.relative_to(LOOM))
        for path in sorted(LOOM.rglob("*.sql"))
        if PLACEHOLDER.search(path.read_text(encoding="utf-8"))
    ]

    assert offenders == []


def test_every_declared_fragment_exemption_matches_a_call_in_the_source() -> None:
    found: set[tuple[str, str, str]] = set()
    for path in _modules():
        functions = _enclosing_functions(path)
        relative = path.relative_to(SRC).as_posix()
        for _, call in _sink_calls(path):
            function = functions.get((call.lineno, call.col_offset), "")
            found.add((relative, function, ast.unparse(call)))

    assert DECLARED_DDL_FRAGMENTS - found == set()


@pytest.mark.parametrize(
    ("function", "violations"),
    [
        ("_partial_unique_index", ["3: CheckConstraint(expression, name=rule)"]),
        ("anywhere_else", ["2: text(partial.where)", "3: CheckConstraint(expression, name=rule)"]),
    ],
)
def test_a_declared_fragment_is_exempt_only_in_its_own_function(
    function: str, violations: list[str], tmp_path: Path
) -> None:
    module = tmp_path / "loom" / "core" / "backend" / "sqlalchemy.py"
    module.parent.mkdir(parents=True)
    module.write_text(
        f"def {function}(partial, expression, rule):\n"
        "    text(partial.where)\n"
        "    CheckConstraint(expression, name=rule)\n",
        encoding="utf-8",
    )

    assert _violations(module, root=tmp_path) == violations
