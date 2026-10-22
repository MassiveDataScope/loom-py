"""The Polars provider must load and wire without ``pymongo`` installed."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from loom.core.connections import MongoConnection
from loom.etl.storage._config import StorageConfig, StorageDefaults, TablePathConfig
from tests.helpers.extras import MONGO_ISLAND, without_extra


def test_provider_imports_with_pymongo_hidden(tmp_path: Path) -> None:
    script = textwrap.dedent(
        f"""
        import sys

        sys.modules["pymongo"] = None
        from loom.etl.backends.polars.provider import PolarsProvider
        from loom.etl.storage._config import StorageConfig, StorageDefaults, TablePathConfig

        config = StorageConfig(
            engine="polars",
            defaults=StorageDefaults(table_path=TablePathConfig(uri={str(tmp_path)!r})),
        )
        reader, writer = PolarsProvider().create_backends(config)
        print(type(reader).__name__, type(writer).__name__)
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["PolarsSourceReader", "PolarsTargetWriter"]


def test_mongo_source_without_pymongo_names_the_extra(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from loom.etl.backends.polars.provider import PolarsProvider

    without_extra(monkeypatch, ["pymongo"], [MONGO_ISLAND])
    config = StorageConfig(
        engine="polars",
        defaults=StorageDefaults(table_path=TablePathConfig(uri=str(tmp_path))),
        mongo=MongoConnection(uri="mongodb://localhost:27017", database="app"),
    )

    provider = PolarsProvider()

    with pytest.raises(ImportError, match=r"loom-kernel\[mongo\]"):
        provider.create_backends(config)
