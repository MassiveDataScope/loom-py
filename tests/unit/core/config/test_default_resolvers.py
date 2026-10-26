"""Tests for ``default_resolvers``, ``merge_resolvers`` and registration precedence."""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from omegaconf import OmegaConf
from omegaconf.errors import InterpolationResolutionError

from loom.core.config import (
    ConfigError,
    ConfigResolver,
    default_resolvers,
    load_config,
    merge_resolvers,
    with_default_resolvers,
)
from tests.helpers.extras import with_boto3, without_boto3


class StubResolver:
    def __init__(self, name: str, prefix: str) -> None:
        self._name = name
        self._prefix = prefix

    @property
    def name(self) -> str:
        return self._name

    def resolve(self, key: str) -> object:
        return f"{self._prefix}:{key}"


def _names(resolvers: tuple[ConfigResolver, ...]) -> tuple[str, ...]:
    return tuple(r.name for r in resolvers)


@pytest.fixture
def secrets_yaml(tmp_path: Path) -> str:
    path = tmp_path / "app.yaml"
    path.write_text("token: ${secrets:k}\n")
    return str(path)


@pytest.fixture
def plain_yaml(tmp_path: Path) -> str:
    path = tmp_path / "plain.yaml"
    path.write_text("app:\n  name: demo\n")
    return str(path)


def test_load_config_registers_no_default_resolvers(plain_yaml: str) -> None:
    load_config(plain_yaml)

    assert not OmegaConf.has_resolver("secrets")
    assert not OmegaConf.has_resolver("ssm")


def test_explicit_resolver_replaces_earlier_registration(secrets_yaml: str) -> None:
    first = load_config(secrets_yaml, resolvers=[StubResolver("secrets", "first")])
    assert first.token == "first:k"

    second = load_config(secrets_yaml, resolvers=[StubResolver("secrets", "second")])

    assert second.token == "second:k"


def test_merge_resolvers_drops_default_taken_by_explicit() -> None:
    vault = StubResolver("secrets", "vault")

    merged = merge_resolvers([vault], default_resolvers())

    assert merged[0] is vault
    assert _names(merged) == ("secrets", "ssm")


def test_merge_resolvers_drops_default_already_registered(secrets_yaml: str) -> None:
    load_config(secrets_yaml, resolvers=[StubResolver("secrets", "vault")])

    merged = merge_resolvers((), default_resolvers())

    assert _names(merged) == ("ssm",)
    assert load_config(secrets_yaml, resolvers=merged).token == "vault:k"


def test_merge_resolvers_logs_skipped_default_already_registered(
    secrets_yaml: str, caplog: pytest.LogCaptureFixture
) -> None:
    load_config(secrets_yaml, resolvers=[StubResolver("secrets", "vault")])

    with caplog.at_level(logging.DEBUG, logger="loom.core.config.resolver"):
        merge_resolvers((), default_resolvers())

    assert caplog.messages == ["config resolver 'secrets' already registered; loom default skipped"]


def test_merge_resolvers_does_not_log_default_taken_by_explicit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG, logger="loom.core.config.resolver"):
        merge_resolvers([StubResolver("secrets", "vault")], default_resolvers())

    assert caplog.messages == []


def test_with_default_resolvers_matches_merge_of_defaults() -> None:
    vault = StubResolver("secrets", "vault")

    merged = with_default_resolvers([vault])

    assert merged[0] is vault
    assert _names(merged) == ("secrets", "ssm")
    assert _names(with_default_resolvers()) == ("secrets", "ssm")


def test_importing_config_package_does_not_import_omegaconf() -> None:
    script = "import sys; import loom.core.config; print('omegaconf' in sys.modules)"

    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == "False"


def test_merge_resolvers_keeps_free_defaults() -> None:
    merged = merge_resolvers((), default_resolvers())

    assert _names(merged) == ("secrets", "ssm")


def test_merge_resolvers_keeps_explicit_order() -> None:
    a = StubResolver("a", "a")
    b = StubResolver("b", "b")

    assert merge_resolvers([a, b], ()) == (a, b)


def test_default_resolvers_construct_without_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    without_boto3(monkeypatch)

    resolvers = default_resolvers()

    assert _names(resolvers) == ("secrets", "ssm")
    for resolver in resolvers:
        with pytest.raises(ConfigError, match=r"loom-kernel\[config-ssm\]"):
            resolver.resolve("/x")


def test_no_client_created_without_placeholders(
    plain_yaml: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    boto3 = MagicMock()
    with_boto3(monkeypatch, boto3)

    cfg = load_config(plain_yaml, resolvers=default_resolvers())

    assert cfg.app.name == "demo"
    boto3.Session.assert_not_called()


def test_default_secrets_resolver_reports_the_extra_on_access(
    secrets_yaml: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    without_boto3(monkeypatch)
    cfg = load_config(secrets_yaml, resolvers=default_resolvers())

    with pytest.raises(InterpolationResolutionError, match=r"loom-kernel\[config-ssm\]"):
        _ = cfg.token
