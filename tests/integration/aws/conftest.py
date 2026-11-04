"""loom's AWS integrations against the AWS emulator of ``docker-compose.local.yaml``.

Gating: ``LOOM_AWS_IT_URL`` names the emulator endpoint; unset or
unreachable, every test here skips, naming the command that starts it.
Each test configures loom through a YAML file whose ``aws:`` section points
at the emulator, so every product reaches it by inheriting that section, and
names its own resources with a uuid so reruns never collide.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

import boto3  # type: ignore[import-untyped]
import pytest
import yaml
from botocore.config import Config  # type: ignore[import-untyped]
from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]

URL_ENV_VAR = "LOOM_AWS_IT_URL"
COMPOSE_COMMAND = "docker compose -f docker-compose.local.yaml up -d localstack"
REGION = "us-east-1"
_DUMMY_CREDENTIALS = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}

AwsClientFactory = Callable[[str], Any]
ConfigWriter = Callable[[dict[str, Any]], Path]


def unique(prefix: str) -> str:
    """Return *prefix* with a uuid suffix: a resource name no other run uses."""
    return f"{prefix}-{uuid4().hex}"


def _client(url: str, service: str) -> Any:
    return boto3.client(
        service,
        endpoint_url=url,
        region_name=REGION,
        config=Config(connect_timeout=2, read_timeout=10, retries={"total_max_attempts": 1}),
        **_DUMMY_CREDENTIALS,
    )


@pytest.fixture(scope="session")
def aws_url() -> str:
    url = os.environ.get(URL_ENV_VAR)
    if not url:
        pytest.skip(
            f"{URL_ENV_VAR} is not set; start the AWS emulator with `{COMPOSE_COMMAND}` "
            "and export it"
        )
    try:
        _client(url, "sqs").list_queues()
    except (BotoCoreError, ClientError) as exc:
        pytest.skip(
            f"the AWS emulator at ${URL_ENV_VAR} is not reachable ({type(exc).__name__});"
            f" start it with `{COMPOSE_COMMAND}`"
        )
    return url


@pytest.fixture(autouse=True)
def _emulator_credentials(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Dummy keys for boto3's chain; region and endpoint can only come from ``aws:``."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", _DUMMY_CREDENTIALS["aws_access_key_id"])
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _DUMMY_CREDENTIALS["aws_secret_access_key"])
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-aws-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-aws-credentials"))
    for name in (
        "AWS_PROFILE",
        "AWS_SESSION_TOKEN",
        "AWS_ENDPOINT_URL",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def aws(aws_url: str) -> AwsClientFactory:
    """Build a boto3 client on the emulator, to create and inspect what loom uses."""
    return lambda service: _client(aws_url, service)


@pytest.fixture
def write_config(tmp_path: Path, aws_url: str) -> ConfigWriter:
    """Write *sections* to a YAML file under ``aws: {url, region}`` and return its path."""

    def write(sections: dict[str, Any]) -> Path:
        path = tmp_path / "config.yaml"
        document = {"aws": {"url": aws_url, "region": REGION}, **sections}
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        return path

    return write
