"""One definition per engine connection, one place that builds each engine's client.

Both checks walk the source tree. A class declaring the keys that identify an
engine's connection must be that engine's type; a context extends it instead
of declaring the keys again (SC-001). A driver client is built only inside
the ``client`` island of the engine's package of :mod:`loom.core.connections`;
every other module goes through it (SC-002).
"""

from __future__ import annotations

import ast
from pathlib import Path

_SRC = Path(__file__).resolve().parents[4] / "src" / "loom"

# The keys that identify each engine's connection, and the one type declaring them.
_SIGNATURES: dict[frozenset[str], str] = {
    frozenset({"region", "url"}): "core/connections/aws/config.py:AwsClientConfig",
    frozenset({"url", "database", "server_selection_timeout"}): (
        "core/connections/mongo/config.py:MongoConnection"
    ),
    frozenset({"url", "password", "executor_threads"}): (
        "core/connections/clickhouse/config.py:ClickHouseConnection"
    ),
    frozenset({"url", "connect_args"}): "core/persistence/database.py:DatabaseConfig",
}

_DRIVER_MODULES = frozenset({"boto3", "clickhouse_connect", "pymongo"})
_DRIVER_FACTORIES = frozenset({"client", "resource", "Session", "get_client", "get_async_client"})
_DRIVER_CLASSES = frozenset({"MongoClient", "AsyncMongoClient"})
_BUILDERS = frozenset(
    {
        "core/connections/aws/client.py",
        "core/connections/clickhouse/client.py",
        "core/connections/mongo/client.py",
    }
)


def _modules() -> list[tuple[str, ast.Module]]:
    return [
        (path.relative_to(_SRC).as_posix(), ast.parse(path.read_text(encoding="utf-8")))
        for path in sorted(_SRC.rglob("*.py"))
    ]


def _declared_fields(node: ast.ClassDef) -> set[str]:
    return {
        statement.target.id
        for statement in node.body
        if isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name)
    }


def test_every_connection_key_set_is_declared_by_one_type() -> None:
    misplaced = []
    for relative, tree in _modules():
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            fields = _declared_fields(node)
            for signature, owner in _SIGNATURES.items():
                if signature <= fields and f"{relative}:{node.name}" != owner:
                    misplaced.append(f"{relative}:{node.name} declares {sorted(signature)}")

    assert misplaced == []


def _driver_bindings(tree: ast.Module) -> tuple[set[str], set[str]]:
    """Return the names bound to a driver module and to a driver factory or class.

    Covers ``import boto3 as b``, ``from boto3 import session``,
    ``from clickhouse_connect import get_client as gc`` and
    ``from pymongo import MongoClient as MC``.
    """
    modules: set[str] = set()
    factories: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in _DRIVER_MODULES:
                    modules.add((alias.asname or alias.name).split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            if node.module.split(".")[0] not in _DRIVER_MODULES:
                continue
            for alias in node.names:
                bound = alias.asname or alias.name
                if alias.name in _DRIVER_FACTORIES | _DRIVER_CLASSES:
                    factories.add(bound)
                else:
                    modules.add(bound)
    return modules, factories


def _root(expression: ast.expr) -> str | None:
    while isinstance(expression, ast.Attribute):
        expression = expression.value
    return expression.id if isinstance(expression, ast.Name) else None


def _builds_a_driver_client(call: ast.Call, modules: set[str], factories: set[str]) -> bool:
    function = call.func
    if isinstance(function, ast.Name):
        return function.id in factories or function.id in _DRIVER_CLASSES
    if isinstance(function, ast.Attribute):
        named = function.attr in _DRIVER_FACTORIES | _DRIVER_CLASSES
        return named and _root(function.value) in modules
    return False


def test_driver_clients_are_built_only_by_the_builders() -> None:
    outside = []
    for relative, tree in _modules():
        if relative in _BUILDERS:
            continue
        modules, factories = _driver_bindings(tree)
        outside.extend(
            f"{relative}:{node.lineno}"
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and _builds_a_driver_client(node, modules, factories)
        )

    assert outside == []


def test_the_driver_call_check_sees_aliased_and_nested_forms() -> None:
    sample = ast.parse(
        "import boto3 as b\n"
        "from pymongo import MongoClient as MC\n"
        "from clickhouse_connect import get_client\n"
        "import pymongo\n"
        "b.session.Session()\n"
        "MC('mongodb://h')\n"
        "get_client(dsn='x')\n"
        "pymongo.MongoClient('mongodb://h')\n"
        "prefect_client.get_client()\n"
    )
    modules, factories = _driver_bindings(sample)

    found = [
        node.lineno
        for node in ast.walk(sample)
        if isinstance(node, ast.Call) and _builds_a_driver_client(node, modules, factories)
    ]
    assert sorted(found) == [5, 6, 7, 8]
