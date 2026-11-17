"""Boto3 clients for moto, built once per process.

A client holds no AWS state: moto keeps it in backends it resets whenever ``mock_aws()``
starts and stops, and intercepts requests through a handler botocore registers for every
client. Sharing the clients therefore saves botocore a service-catalogue scan per test.
"""

from __future__ import annotations

from functools import cache
from typing import Any

import boto3

REGION = "us-east-1"


@cache
def moto_client(service: str, region: str = REGION) -> Any:
    """The client of *service* in *region* with fake credentials, for use under ``mocked_aws``."""
    return boto3.Session().client(
        service,
        region_name=region,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
