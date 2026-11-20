"""The AWS side of the test environment: a bare environment, moto clients and a moto probe."""

from __future__ import annotations

import os
from functools import cache
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.loaders import Loader

from loom.core.connections.aws import AwsClientConfig
from loom.core.model.redaction import reveal_for_driver
from loom.testing._moto import UNANSWERED, unanswered

REGION = "us-east-1"
OTHER_REGION = "eu-west-1"
ACCOUNT = "123456789012"
FAKE_KEY = "testing"
_ABSENT_AWS_DIR = Path("/nonexistent/loom-tests-aws")


def scrub_aws_environment(patch: pytest.MonkeyPatch) -> None:
    """Remove every ``AWS_*``, ``BOTO_*`` and ``MOTO_*`` variable and set only the minimum.

    The minimum is absent config, credentials and model files, fake keys and no metadata service.
    """
    for name in tuple(os.environ):
        if name.startswith(("AWS_", "BOTO_", "MOTO_")):
            patch.delenv(name)
    patch.setenv("AWS_CONFIG_FILE", str(_ABSENT_AWS_DIR / "config"))
    patch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(_ABSENT_AWS_DIR / "credentials"))
    patch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    patch.setenv("AWS_ACCESS_KEY_ID", FAKE_KEY)
    patch.setenv("AWS_SECRET_ACCESS_KEY", FAKE_KEY)
    patch.setattr(Loader, "CUSTOMER_DATA_PATH", str(_ABSENT_AWS_DIR / "models"))


def build_moto_client(
    service: str, region: str = REGION, config: AwsClientConfig | None = None
) -> Any:
    """Build a client of *service* in *region* in the bare environment, for use under moto.

    Args:
        service: The boto3 service name.
        region: The region of the client.
        config: Settings whose bounds the client takes, when given.
    """
    options = {} if config is None else reveal_for_driver(config.client_options())
    bounds = Config(**options["config"]) if options else None
    with pytest.MonkeyPatch.context() as patch:
        scrub_aws_environment(patch)
        return boto3.Session().client(service, region_name=region, config=bounds)


@cache
def moto_client(service: str, region: str = REGION) -> Any:
    """Return the shared client of *service* in *region*, which a test must not change."""
    return build_moto_client(service, region)


@cache
def _moto_probe() -> Any:
    client = build_moto_client("sqs")
    client.meta.events.register_last(
        "before-send", lambda request, **_: unanswered(request, "moto is stopped")
    )
    return client


def is_moto_answering() -> bool:
    """Return whether moto answers a request now."""
    try:
        _moto_probe().list_queues()
    except ClientError as error:
        if error.response["Error"]["Code"] != UNANSWERED:
            raise
        return False
    return True
