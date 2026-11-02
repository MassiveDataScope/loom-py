"""Connection settings import no driver; a driver is imported only by its island.

The contracts of :mod:`loom.core.connections` are read by every layer, so
importing them must load no SDK. The import check runs in a clean
interpreter, since the pytest process has already imported the drivers for
other suites. The source check walks the tree: a driver SDK appears in an
``import`` only inside the modules allowed to hold it (the island rule of
``docs/architecture/clean-architecture.md``). An import under
``if TYPE_CHECKING:`` loads nothing at run time, so a loader naming the
driver's types in its Protocol is no island.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

from tests.helpers.imports import imported_roots

_SRC = Path(__file__).resolve().parents[2] / "src"

_SDKS = ("boto3", "botocore", "clickhouse_connect", "pymongo")

_SCRIPT = """
import json
import sys

import loom.core.connections
import loom.core.connections.aws
import loom.core.connections.aws.config
import loom.core.connections.aws.loader
import loom.core.connections.clickhouse
import loom.core.connections.clickhouse.config
import loom.core.connections.clickhouse.loader
import loom.core.connections.mongo
import loom.core.connections.mongo.config
import loom.core.connections.mongo.loader

sdks = {sdks!r}
print(json.dumps(sorted(
    name for name in sys.modules
    if any(name == root or name.startswith(root + ".") for root in sdks)
)))
""".replace("{sdks!r}", repr(_SDKS))

# The exact modules allowed to import a driver SDK, under src/loom. A new
# module importing one is either a new island, named here on purpose, or a
# leak this test catches.
_ISLANDS: dict[str, frozenset[str]] = {
    "boto3": frozenset(
        {
            "core/connections/aws/client.py",
            "core/repository/dynamodb/repository.py",  # item serializers, conditions
            "etl/io/sources/_dynamodb_items.py",  # item deserializers
            "etl/io/sources/_dynamodb_predicate.py",  # condition builders
        }
    ),
    "botocore": frozenset(
        {
            "core/connections/aws/client.py",
            "core/repository/dynamodb/repository.py",  # error types
            "prefect/notify/_sns.py",  # error types
        }
    ),
    "clickhouse_connect": frozenset(
        {
            "core/connections/clickhouse/client.py",
            "core/sql/clickhouse/_client.py",  # error types, multi-role transport
        }
    ),
    "pymongo": frozenset(
        {
            "core/connections/mongo/client.py",
            "core/repository/mongo/backend.py",  # client type of the backend
            "core/repository/mongo/repository.py",  # results, errors
            "core/repository/mongo/uow.py",  # sessions
            "streaming/mongo/_bytewax_source.py",  # change-stream errors
        }
    ),
}


def test_importing_the_connection_contracts_loads_no_sdk() -> None:
    result = subprocess.run(
        # Not -I: it ignores PYTHONPATH, and the check must run against this tree.
        [sys.executable, "-s", "-c", _SCRIPT],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(_SRC)},
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.splitlines()[-1]) == []


def test_a_driver_sdk_is_imported_only_by_its_islands() -> None:
    """Static imports only; a dynamic ``importlib`` load is what the island rule forbids."""
    offenders = []
    for path in sorted((_SRC / "loom").rglob("*.py")):
        relative = path.relative_to(_SRC / "loom").as_posix()
        for sdk in imported_roots(ast.parse(path.read_text(encoding="utf-8"))) & set(_ISLANDS):
            if relative not in _ISLANDS[sdk]:
                offenders.append(f"{relative} imports {sdk}")

    assert offenders == []


def test_the_import_check_skips_type_only_imports() -> None:
    sample = ast.parse(
        "from typing import TYPE_CHECKING\n"
        "import typing\n"
        "if TYPE_CHECKING:\n"
        "    import pymongo\n"
        "if typing.TYPE_CHECKING:\n"
        "    import boto3\n"
        "else:\n"
        "    import botocore\n"
        "import clickhouse_connect\n"
    )

    assert imported_roots(sample) == {"typing", "botocore", "clickhouse_connect"}


def test_reading_configuration_loads_no_persistence_layer() -> None:
    """The resolvers name a connection type; that must not pull persistence into config."""
    script = (
        "import json, sys\n"
        "import loom.core.config\n"
        "print(json.dumps(sorted(m for m in sys.modules if m.startswith("
        "('loom.core.persistence', 'loom.core.uow', 'loom.core.di')))))\n"
    )
    result = subprocess.run(
        [sys.executable, "-s", "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(_SRC)},
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.splitlines()[-1]) == []
