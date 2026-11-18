"""``AwsClients``: one client per service and distinct settings, built once, closed together."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any

import pytest

from loom.core.config._resolver_utils import build_resolver_client
from loom.core.connections.aws import AwsClientConfig, AwsClients, AwsPool
from loom.core.connections.aws.loader import aws_island
from loom.core.model.redaction import Dsn
from loom.core.plugins.optional import MissingExtraError
from loom.serverless.aws.lambda_ import _jobs
from tests.helpers.aws import REGION
from tests.helpers.aws_island import FakeAwsIsland, FakeSdkError

_THREADS = 8


_CONFIG = AwsClientConfig(region="eu-west-1")


@pytest.fixture
def clients() -> AwsClients:
    return AwsClients()


class TestFakeIsland:
    def test_every_importer_of_the_island_resolves_the_fake(self, island: FakeAwsIsland) -> None:
        assert aws_island() is island
        assert _jobs.aws_island() is island

    def test_an_island_of_mock_clients_reports_only_its_own_error(
        self, island: FakeAwsIsland
    ) -> None:
        assert aws_island().sdk_errors() == (FakeSdkError,)


class TestClientCache:
    def test_the_same_service_and_settings_give_the_same_client(
        self, island: FakeAwsIsland, clients: AwsClients
    ) -> None:
        first = clients.client("dynamodb", _CONFIG)

        assert clients.client("dynamodb", _CONFIG) is first
        assert clients.client("dynamodb", AwsClientConfig(region="eu-west-1")) is first
        assert len(island.built) == 1

    def test_another_service_gives_another_client(
        self, island: FakeAwsIsland, clients: AwsClients
    ) -> None:
        assert clients.client("dynamodb", _CONFIG) is not clients.client("sqs", _CONFIG)

    @pytest.mark.parametrize(
        "other",
        [
            AwsClientConfig(region=REGION),
            AwsClientConfig(region="eu-west-1", url=Dsn("http://localhost:4566")),
            AwsClientConfig(region="eu-west-1", pool=AwsPool(size=2)),
            AwsClientConfig(region="eu-west-1", read_timeout=30),
        ],
    )
    def test_other_settings_give_another_client(
        self, island: FakeAwsIsland, clients: AwsClients, other: AwsClientConfig
    ) -> None:
        assert clients.client("dynamodb", other) is not clients.client("dynamodb", _CONFIG)

    def test_a_subclass_of_the_settings_shares_the_client_of_its_base(
        self, island: FakeAwsIsland, clients: AwsClients
    ) -> None:
        class Queued(AwsClientConfig, frozen=True, kw_only=True):
            queues: dict[str, str]

        base = clients.client("sqs", AwsClientConfig(region="eu-west-1"))

        assert clients.client("sqs", Queued(region="eu-west-1", queues={"a": "u"})) is base

    def test_one_session_serves_every_client_of_a_profile(
        self, island: FakeAwsIsland, clients: AwsClients
    ) -> None:
        clients.client("dynamodb", _CONFIG)
        clients.client("sqs", _CONFIG)
        clients.client("sqs", AwsClientConfig(region=REGION))
        clients.client("sns", AwsClientConfig(profile="dev"))

        assert island.sessions == [None, "dev"]

    def test_a_missing_boto3_names_the_aws_extra(
        self, clients: AwsClients, absent_boto3: None
    ) -> None:

        with pytest.raises(MissingExtraError, match=r"needs the 'aws' extra"):
            clients.client("s3", _CONFIG)


class TestConcurrentFirstCalls:
    def test_threads_asking_at_once_build_exactly_one_client(
        self, monkeypatch: pytest.MonkeyPatch, clients: AwsClients
    ) -> None:
        island = FakeAwsIsland(build_seconds=0.05).install(monkeypatch)
        start = Barrier(_THREADS)

        def ask(_: int) -> Any:
            start.wait()
            return clients.client("dynamodb", _CONFIG)

        with ThreadPoolExecutor(_THREADS) as pool:
            answers = list(pool.map(ask, range(_THREADS)))

        assert len(island.built) == 1
        assert all(answer is answers[0] for answer in answers)


class _PooledClient:
    """Holds a pooled connection from its first call until it is closed."""

    def __init__(self) -> None:
        self.connected = False
        self.closes = 0

    def call(self) -> None:
        self.connected = True

    def close(self) -> None:
        self.connected = False
        self.closes += 1


class _PooledIsland(FakeAwsIsland):
    def __init__(self) -> None:
        super().__init__()
        self.pooled: list[_PooledClient] = []

    def build_aws_client(self, session: object, service: str, config: Any) -> Any:
        super().build_aws_client(session, service, config)
        client = _PooledClient()
        self.pooled.append(client)
        return client


class TestClose:
    @pytest.fixture
    def pooled(self, monkeypatch: pytest.MonkeyPatch) -> _PooledIsland:
        island = _PooledIsland()
        monkeypatch.setattr("loom.core.connections.aws.clients.aws_island", lambda: island)
        return island

    def test_a_reopened_client_is_the_same_and_the_second_close_releases_its_connection(
        self, pooled: _PooledIsland, clients: AwsClients
    ) -> None:
        client = clients.client("dynamodb", _CONFIG)
        client.call()

        clients.close()
        reopened = clients.client("dynamodb", _CONFIG)
        reopened.call()
        assert reopened.connected
        clients.close()

        assert reopened is client
        assert (client.connected, client.closes) == (False, 2)
        assert len(pooled.pooled) == 1

    def test_close_keeps_the_sessions(self, island: FakeAwsIsland, clients: AwsClients) -> None:
        clients.client("dynamodb", _CONFIG)
        clients.close()
        clients.client("sqs", _CONFIG)

        assert island.sessions == [None]

    def test_every_client_is_closed_even_when_one_fails_and_the_failures_are_grouped(
        self, island: FakeAwsIsland, clients: AwsClients
    ) -> None:
        failing, other = clients.client("dynamodb", _CONFIG), clients.client("sqs", _CONFIG)
        failing.close.side_effect = OSError("boom")

        with pytest.raises(ExceptionGroup) as raised:
            clients.close()

        other.close.assert_called_once_with()
        assert raised.value.exceptions == (failing.close.side_effect,)


class TestRealIslandPath:
    def test_the_region_and_endpoint_of_the_settings_reach_boto3(self, mocked_aws: None) -> None:
        settings = AwsClientConfig(region="eu-west-1", url=Dsn("http://localhost:4566"))

        client = AwsClients().client("sqs", settings)

        assert client.meta.region_name == "eu-west-1"
        assert client.meta.endpoint_url == "http://localhost:4566"


class TestWithoutAContainer:
    def test_the_resolver_path_builds_its_client_with_clients_of_its_own(
        self, mocked_aws: None
    ) -> None:
        client = build_resolver_client("ssm", "eu-west-3", resolver="SsmResolver", root=None)

        assert client.meta.service_model.service_name == "ssm"
        assert client.meta.region_name == "eu-west-3"
