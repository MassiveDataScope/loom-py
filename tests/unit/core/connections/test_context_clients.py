"""What a context declares on its connection reaches the driver client it builds.

Before one builder per engine existed, credentials and bounds reached the
driver only from the API; the ETL and the streaming sinks dropped them.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.core.connections import AwsClientConfig, ClickHouseConnection
from tests.helpers.extras import patched_aws_clients

_CONNECTION = ClickHouseConnection(
    url="clickhouse://h:8123/db", username="etl", password="pw", send_receive_timeout=600
)


@pytest.fixture
def get_client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    recorder = MagicMock()
    monkeypatch.setattr("clickhouse_connect.get_client", recorder)
    return recorder


def _driver_options(recorder: MagicMock) -> dict[str, Any]:
    return dict(recorder.call_args.kwargs)


def test_the_etl_target_hands_credentials_and_bounds_to_the_driver(get_client: MagicMock) -> None:
    from loom.etl.io.targets._clickhouse import ClickHouseTargetWriter

    ClickHouseTargetWriter(connection=_CONNECTION)

    assert _driver_options(get_client) == {
        "dsn": "clickhouse://h:8123/db",
        "username": "etl",
        "password": "pw",
        "send_receive_timeout": 600,
    }


def test_the_etl_source_and_client_step_hand_over_the_same_connection(
    get_client: MagicMock,
) -> None:
    from loom.etl.io.sources._clickhouse import ClickHouseSourceReader
    from loom.etl.io.targets._clickhouse import ClickHouseClientExecutor

    ClickHouseSourceReader(connection=_CONNECTION).command(lambda _client: None)
    source = _driver_options(get_client)
    ClickHouseClientExecutor(connection=_CONNECTION).command(lambda _client: None)

    assert source == _driver_options(get_client)
    assert (source["username"], source["password"]) == ("etl", "pw")


def test_a_bare_dsn_is_a_connection_holding_only_that_dsn(get_client: MagicMock) -> None:
    from loom.etl.io.targets._clickhouse import ClickHouseTargetWriter

    ClickHouseTargetWriter("clickhouse://h:8123/db")

    assert _driver_options(get_client) == {"dsn": "clickhouse://h:8123/db"}


def test_a_component_refuses_a_dsn_and_a_connection_together() -> None:
    from loom.etl.io.targets._clickhouse import ClickHouseClientExecutor

    with pytest.raises(ValueError, match="'url' or 'connection', not both"):
        ClickHouseClientExecutor("clickhouse://h/db", connection=_CONNECTION)


def test_a_streaming_sink_hands_credentials_to_the_driver(get_client: MagicMock) -> None:
    import msgspec

    from loom.streaming.nodes._table._clickhouse import _ClickHouseTablePartition
    from loom.streaming.nodes._table.common import ClickHouseSinkConfig

    class _Row(msgspec.Struct):
        id: int

    settings = ClickHouseSinkConfig.from_config(
        {"table": "t", "insert_timeout": 5}, default_table="t", connection=_CONNECTION
    )
    _ClickHouseTablePartition(settings, _Row)

    options = _driver_options(get_client)
    assert (options["username"], options["password"]) == ("etl", "pw")
    # The connection declares a bound, so the sink's insert_timeout does not replace it.
    assert options["send_receive_timeout"] == 600
    assert "connect_timeout" not in options


def test_a_sink_insert_timeout_bounds_a_connection_declaring_none(get_client: MagicMock) -> None:
    import msgspec

    from loom.streaming.nodes._table._clickhouse import _ClickHouseTablePartition
    from loom.streaming.nodes._table.common import ClickHouseSinkConfig

    class _Row(msgspec.Struct):
        id: int

    settings = ClickHouseSinkConfig.from_config(
        {"table": "t", "insert_timeout": 0.5},
        default_table="t",
        connection=ClickHouseConnection(url="clickhouse://h/db"),
    )
    _ClickHouseTablePartition(settings, _Row)

    options = _driver_options(get_client)
    # A sub-second timeout rounds up: a zero bound would mean "no timeout" to nobody.
    assert (options["connect_timeout"], options["send_receive_timeout"]) == (1, 1)


def test_the_etl_dynamodb_client_takes_the_declared_bounds() -> None:
    from loom.etl.backends.polars.provider import _build_dynamodb_client

    with patched_aws_clients() as clients:
        _build_dynamodb_client(AwsClientConfig(region="eu-west-1", read_timeout=9))

    assert clients.calls == [("dynamodb", "eu-west-1")]
    assert clients.options[-1]["config"].read_timeout == 9
