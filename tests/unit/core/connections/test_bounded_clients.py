"""Every context calling AWS fails within loom's bounds when the service never answers.

botocore alone waits 60 s for a read, longer than a typical serverless
invocation. The repository bound was covered by the DynamoDB suite; the ETL
sources and the resolvers now share the same builder and the same bounds.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import pytest

from loom.connections.aws import AwsClients
from loom.core.connections import AwsClientConfig
from loom.core.model.redaction import Dsn
from tests.helpers.aws import OTHER_REGION
from tests.helpers.silent_endpoint import silent_endpoint  # noqa: F401 - pytest fixture

# (connect + read) x attempts plus at most one second of standard backoff jitter.
# One attempt at the smallest timeouts keeps the test short; the declared
# retries reaching botocore are pinned without a network by test_client_options.
_BOUND = (1 + 1) * 1 + 1


def _elapsed(call: Callable[[], Any]) -> float:
    started = time.perf_counter()
    with pytest.raises(Exception) as raised:
        call()
    elapsed = time.perf_counter() - started
    described = f"{type(raised.value).__name__} {raised.value}".lower()
    assert "timeout" in described or "timed out" in described
    return elapsed


class TestClientsFailWithinDeclaredBounds:
    def test_the_etl_dynamodb_client_fails_within_its_bounds(
        self,
        silent_endpoint: str,  # noqa: F811
    ) -> None:
        from loom.etl.backends.polars.provider import _build_dynamodb_client

        client = _build_dynamodb_client(
            AwsClients(),
            AwsClientConfig(
                region=OTHER_REGION,
                url=Dsn(silent_endpoint),
                connect_timeout=1,
                read_timeout=1,
                max_attempts=1,
            ),
        )

        assert _elapsed(lambda: client.describe_table(TableName="t")) < _BOUND

    @pytest.mark.slow
    def test_the_ssm_resolver_fails_within_the_default_bounds(
        self,
        silent_endpoint: str,  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from loom.connections.aws.ssm import SsmResolver

        # The resolvers take no section; the endpoint comes from boto3's own variable.
        monkeypatch.setenv("AWS_ENDPOINT_URL", silent_endpoint)
        defaults = AwsClientConfig()
        bound = (defaults.connect_timeout + defaults.read_timeout) * defaults.max_attempts + 1

        assert _elapsed(lambda: SsmResolver(OTHER_REGION).resolve("/x")) < bound
