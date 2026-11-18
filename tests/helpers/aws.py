"""The AWS side of the test environment: a bare environment, moto clients, and a moto probe.

A client holds no AWS state: moto keeps it in backends it resets whenever ``mock_aws()``
starts and stops, and intercepts requests through a handler botocore registers for every
client. Sharing the clients therefore saves botocore a service-catalogue scan per test.
"""

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
ACCOUNT = "123456789012"
FAKE_KEY = "testing"
_NOWHERE = Path("/nonexistent/loom-tests-aws")


def scrub_aws_environment(patch: pytest.MonkeyPatch) -> None:
    """Remove every ``AWS_*``, ``BOTO_*`` and ``MOTO_*`` variable, then set only the minimum.

    The minimum is absent config, credentials and model files, fake keys and no metadata
    service. botocore adds ``~/.aws/models`` to its search path from the home directory it
    saw when it was imported, so the loader's own setting is what points it away.
    """
    for name in tuple(os.environ):
        if name.startswith(("AWS_", "BOTO_", "MOTO_")):
            patch.delenv(name)
    patch.setenv("AWS_CONFIG_FILE", str(_NOWHERE / "config"))
    patch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(_NOWHERE / "credentials"))
    patch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    patch.setenv("AWS_ACCESS_KEY_ID", FAKE_KEY)
    patch.setenv("AWS_SECRET_ACCESS_KEY", FAKE_KEY)
    patch.setattr(Loader, "CUSTOMER_DATA_PATH", str(_NOWHERE / "models"))


def build_moto_client(
    service: str, region: str = REGION, config: AwsClientConfig | None = None
) -> Any:
    """A new client of *service* in *region*, built in the bare environment, for use under moto.

    Args:
        service: The boto3 service name.
        region: The region of the client.
        config: Settings whose bounds the client takes, when given.
    """
    options = {} if config is None else reveal_for_driver(config.client_options())
    bounds = Config(**options["config"]) if options else None
    with pytest.MonkeyPatch.context() as patch:
        scrub_aws_environment(patch)
        return boto3.Session().client(
            service,
            region_name=region,
            config=bounds,
            aws_access_key_id=FAKE_KEY,
            aws_secret_access_key=FAKE_KEY,
        )


@cache
def moto_client(service: str, region: str = REGION) -> Any:
    """The shared client of *service* in *region*; a test must not change it.

    It is built in the bare environment whatever the first test to ask had set, and carries
    nothing of the environment afterwards, so one per process is safe.
    """
    return build_moto_client(service, region)


@cache
def _moto_probe() -> Any:
    client = build_moto_client("sqs")
    client.meta.events.register_last(
        "before-send", lambda request, **_: unanswered(request, "moto is stopped")
    )
    return client


def is_moto_answering() -> bool:
    """Whether moto answers a request now: one it leaves unanswered falls to the probe."""
    try:
        _moto_probe().list_queues()
    except ClientError as error:
        if error.response["Error"]["Code"] != UNANSWERED:
            raise
        return False
    return True
