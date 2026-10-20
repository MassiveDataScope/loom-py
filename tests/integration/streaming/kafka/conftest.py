"""Fixtures for the Kafka partitioned-source integration suite.

Runs against a real broker from ``docker-compose.local.yaml``. Two brokers are
provisioned and the same suite runs against both:

- ``redpanda`` (``localhost:19092``) — the fast local and PR loop.
- ``kafka`` (``localhost:19093``) — Apache Kafka in KRaft mode, the fidelity
  check.

Point ``LOOM_KAFKA_IT_BOOTSTRAP`` at whichever broker should be exercised::

    docker compose -f docker-compose.local.yaml up -d redpanda kafka
    pytest tests/integration/streaming/kafka
    LOOM_KAFKA_IT_BOOTSTRAP=localhost:19093 pytest tests/integration/streaming/kafka

Gating: only the broker skips. ``loom.streaming`` is first-party — a breakage
there must FAIL, never skip.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import Callable, Iterator, Sequence

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from loom.core.model import LoomFrozenStruct
from loom.streaming.compiler._plan import CompiledMultiSource, CompiledSingleSource
from loom.streaming.kafka._config import ConsumerSettings
from loom.streaming.kafka._wire import DispatchTable

BOOTSTRAP_ENV_VAR = "LOOM_KAFKA_IT_BOOTSTRAP"
DEFAULT_BOOTSTRAP = "localhost:19092"

_ADMIN_TIMEOUT_S = 10.0


class ItPayload(LoomFrozenStruct, frozen=True):
    """Minimal payload type for building a compiled source.

    The partitioned source never decodes — it emits raw ``KafkaRecord[bytes]``
    — so the payload type only has to exist for the compiled plan to be
    well-formed.

    Attributes:
        value: Arbitrary record body.
    """

    value: str


def _bootstrap_servers() -> str:
    return os.environ.get(BOOTSTRAP_ENV_VAR, DEFAULT_BOOTSTRAP)


def _broker_available(bootstrap: str) -> bool:
    try:
        AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=_ADMIN_TIMEOUT_S)
    except Exception:
        return False
    return True


@pytest.fixture(scope="session")
def bootstrap() -> str:
    """Skip the suite when no Kafka-protocol broker is reachable."""
    servers = _bootstrap_servers()
    if not _broker_available(servers):
        pytest.skip(
            f"no Kafka broker reachable at {servers} — start one with "
            f"'docker compose -f docker-compose.local.yaml up -d redpanda kafka' "
            f"or point {BOOTSTRAP_ENV_VAR} at your own broker"
        )
    return servers


@pytest.fixture
def topic_factory(bootstrap: str) -> Iterator[Callable[[int], str]]:
    """Create uniquely named topics with an explicit partition count.

    Topics are unique per test so a leftover consumer group or an unclean
    teardown can never leak offsets into another test, and every created topic
    is deleted afterwards.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    created: list[str] = []

    def create(partitions: int) -> str:
        name = f"loom.it.{uuid.uuid4().hex[:12]}"
        futures = admin.create_topics(
            [NewTopic(name, num_partitions=partitions, replication_factor=1)]
        )
        futures[name].result(timeout=_ADMIN_TIMEOUT_S)
        created.append(name)
        return name

    yield create

    if created:
        for future in admin.delete_topics(created).values():
            # Teardown must never mask the failure the test already reported.
            with contextlib.suppress(Exception):
                future.result(timeout=_ADMIN_TIMEOUT_S)


@pytest.fixture
def produce(bootstrap: str) -> Callable[[str, Sequence[tuple[int, bytes]]], None]:
    """Publish records to explicit partitions and block until delivered."""
    producer = Producer({"bootstrap.servers": bootstrap})

    def send(topic: str, records: Sequence[tuple[int, bytes]]) -> None:
        for partition, value in records:
            producer.produce(topic=topic, partition=partition, value=value)
        remaining = producer.flush(timeout=_ADMIN_TIMEOUT_S)
        assert remaining == 0, f"{remaining} records were not delivered to {topic}"

    return send


@pytest.fixture
def produce_full(bootstrap: str) -> Callable[..., None]:
    """Publish one record carrying every transport field the contract exposes."""
    producer = Producer({"bootstrap.servers": bootstrap})

    def send(
        topic: str,
        *,
        partition: int,
        key: bytes,
        value: bytes,
        headers: dict[str, bytes],
    ) -> None:
        producer.produce(
            topic=topic,
            partition=partition,
            key=key,
            value=value,
            headers=list(headers.items()),
        )
        remaining = producer.flush(timeout=_ADMIN_TIMEOUT_S)
        assert remaining == 0, f"record was not delivered to {topic}"

    return send


@pytest.fixture
def make_multi_source(bootstrap: str) -> Callable[..., CompiledMultiSource]:
    """Build a compiled heterogeneous source pinned to one topic.

    The partitioned source must accept both compiled source shapes through the
    exact same input contract — it consumes ``settings`` and ``topics`` and
    nothing else, and decoding is a downstream concern.
    """

    def build(topic: str, group: str, **overrides: object) -> CompiledMultiSource:
        settings = ConsumerSettings(
            brokers=(bootstrap,),
            group_id=group,
            topics=(topic,),
            delivery="at_least_once",
            **overrides,  # type: ignore[arg-type]
        )
        return CompiledMultiSource(
            settings=settings,
            topics=(topic,),
            dispatch=DispatchTable(plain={"it": ItPayload}, error={}, wire={}),
            decode_strategy="record",
        )

    return build


@pytest.fixture
def group_id() -> str:
    """Unique consumer group per test — offsets never leak across tests."""
    return f"loom-it-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def make_source(bootstrap: str) -> Callable[..., CompiledSingleSource]:
    """Build a compiled single source pinned to one topic."""

    def build(topic: str, group: str, **overrides: object) -> CompiledSingleSource:
        settings = ConsumerSettings(
            brokers=(bootstrap,),
            group_id=group,
            topics=(topic,),
            delivery="at_least_once",
            **overrides,  # type: ignore[arg-type]
        )
        return CompiledSingleSource(
            settings=settings,
            topics=(topic,),
            payload_type=ItPayload,
            decode_strategy="record",
        )

    return build
