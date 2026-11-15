"""Tests for ``resolvers=`` and the built-in resolver defaults in ``create_app``."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from loom.core.config import ConfigError
from loom.rest.fastapi.auto import create_app
from tests.helpers.extras import with_boto3, without_boto3
from tests.unit._resolver_stubs import MappingResolver
from tests.unit.rest._fixture_app import write_project

pytestmark = pytest.mark.usefixtures("clear_builtin_resolvers")


def _without_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    without_boto3(monkeypatch)


class TestUserResolver:
    def test_user_resolver_reaches_the_app(self, tmp_path: Path) -> None:
        config_path = write_project(tmp_path, rest={"title": "${stub:title}"})

        app = create_app(
            config_path, resolvers=[MappingResolver("stub", {"title": "Resolved API"})]
        )

        assert app.title == "Resolved API"

    def test_user_secrets_resolver_wins_over_the_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _without_boto3(monkeypatch)
        config_path = write_project(tmp_path, rest={"title": "${secrets:/api/title}"})

        app = create_app(
            config_path, resolvers=[MappingResolver("secrets", {"/api/title": "From vault"})]
        )

        assert app.title == "From vault"


class TestSecretsPlaceholder:
    def test_secrets_placeholder_without_boto3_names_the_extra(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _without_boto3(monkeypatch)
        config_path = write_project(tmp_path, rest={"title": "${secrets:/api/title}"})

        with pytest.raises(ConfigError, match=r"loom-kernel\[config-ssm\]"):
            create_app(config_path)

    def test_no_aws_client_without_placeholders(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        boto3 = MagicMock()
        with_boto3(monkeypatch, boto3)
        config_path = write_project(tmp_path)

        assert create_app(config_path) is not None
        boto3.client.assert_not_called()
