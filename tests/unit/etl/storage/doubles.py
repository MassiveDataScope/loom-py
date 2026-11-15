"""Shared doubles for storage tests."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from loom.etl.runner.core import ETLRunner
from loom.etl.storage import StorageConfig


def captured_storage_config(build: Any) -> StorageConfig:
    with patch.object(ETLRunner, "_assemble") as assemble:
        build()
    config = assemble.call_args.args[0]
    return config
