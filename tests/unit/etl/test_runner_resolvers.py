"""Tests for ``resolvers=`` and the built-in resolver defaults in ``ETLRunner.from_yaml``."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from loom.core.config import ConfigError
from loom.etl.runner import ETLRunner
from loom.etl.storage._config import StorageConfig
from tests.helpers.extras import with_boto3, without_boto3
from tests.unit._resolver_stubs import MappingResolver

pytestmark = pytest.mark.usefixtures("clear_builtin_resolvers")


def _write_etl_yaml(tmp_path: Path, uri: str) -> str:
    path = tmp_path / "loom.yaml"
    path.write_text(
        f"storage:\n  defaults:\n    table_path:\n      uri: {uri}\n",
        encoding="utf-8",
    )
    return str(path)


def _captured_config(build: Callable[[], object]) -> StorageConfig:
    with patch.object(ETLRunner, "_assemble") as assemble:
        build()
    config = assemble.call_args.args[0]
    assert isinstance(config, StorageConfig)
    return config


def _lake_uri(config: StorageConfig) -> str:
    assert config.defaults.table_path is not None
    return config.defaults.table_path.uri


def _without_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    without_boto3(monkeypatch)


def test_user_resolver_reaches_the_storage_config(tmp_path: Path) -> None:
    config_path = _write_etl_yaml(tmp_path, "${stub:lake}")
    stub = MappingResolver("stub", {"lake": "s3://lake/resolved"})

    config = _captured_config(lambda: ETLRunner.from_yaml(config_path, resolvers=[stub]))

    assert _lake_uri(config) == "s3://lake/resolved"


def test_secrets_placeholder_without_boto3_names_the_extra(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _without_boto3(monkeypatch)
    config_path = _write_etl_yaml(tmp_path, "${secrets:/etl/lake}")

    with pytest.raises(ConfigError, match=r"loom-kernel\[config-ssm\]"):
        ETLRunner.from_yaml(config_path)


def test_no_aws_client_without_placeholders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    boto3 = MagicMock()
    with_boto3(monkeypatch, boto3)
    config_path = _write_etl_yaml(tmp_path, "/lake")

    config = _captured_config(lambda: ETLRunner.from_yaml(config_path))

    assert _lake_uri(config) == "/lake"
    boto3.client.assert_not_called()


def test_user_secrets_resolver_wins_over_the_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _without_boto3(monkeypatch)
    config_path = _write_etl_yaml(tmp_path, "${secrets:/etl/lake}")
    vault = MappingResolver("secrets", {"/etl/lake": "s3://lake/vault"})

    config = _captured_config(lambda: ETLRunner.from_yaml(config_path, resolvers=[vault]))

    assert _lake_uri(config) == "s3://lake/vault"
