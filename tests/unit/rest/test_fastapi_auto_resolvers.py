"""Tests for ``resolvers=`` and the built-in resolver defaults in ``create_app``."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.core.config import ConfigError
from loom.rest.fastapi.auto import create_app
from tests.helpers.aws_island import FakeAwsIsland
from tests.unit._resolver_stubs import MappingResolver
from tests.unit.rest._fixture_app import write_project

pytestmark = pytest.mark.usefixtures("clear_builtin_resolvers")


class TestUserResolver:
    def test_user_resolver_reaches_the_app(self, tmp_path: Path) -> None:
        config_path = write_project(tmp_path, rest={"title": "${stub:title}"})

        app = create_app(
            config_path, resolvers=[MappingResolver("stub", {"title": "Resolved API"})]
        )

        assert app.title == "Resolved API"

    def test_user_secrets_resolver_wins_over_the_default(
        self, tmp_path: Path, absent_boto3: None
    ) -> None:
        config_path = write_project(tmp_path, rest={"title": "${secrets:/api/title}"})

        app = create_app(
            config_path, resolvers=[MappingResolver("secrets", {"/api/title": "From vault"})]
        )

        assert app.title == "From vault"


class TestSecretsPlaceholder:
    def test_secrets_placeholder_without_boto3_names_the_extra(
        self, tmp_path: Path, absent_boto3: None
    ) -> None:
        config_path = write_project(tmp_path, rest={"title": "${secrets:/api/title}"})

        with pytest.raises(ConfigError, match=r"loom-kernel\[aws\]"):
            create_app(config_path)

    def test_no_aws_client_without_placeholders(
        self, tmp_path: Path, island: FakeAwsIsland
    ) -> None:
        config_path = write_project(tmp_path)

        assert create_app(config_path) is not None
        assert island.sessions == []
