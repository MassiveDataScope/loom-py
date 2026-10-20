from __future__ import annotations

import msgspec
import pytest

from loom.core.routing import LogicalRef
from loom.streaming.kafka import (
    ConsumerSettings,
    KafkaSecuritySettings,
    KafkaSettings,
    ProducerSettings,
    resolve_consumer_topics,
    resolve_producer_topic,
)

pytestmark = pytest.mark.kafka

_TEST_SASL_USERNAME = "test-user"
_TEST_SASL_SECRET = "test-secret"
_TEST_CA_LOCATION = "/etc/ssl/certs/test-ca.pem"


def test_producer_settings_compile_to_confluent_config() -> None:
    settings = ProducerSettings(
        brokers=("k1:9092", "k2:9092"),
        client_id="producer-a",
        security=KafkaSecuritySettings(
            protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username=_TEST_SASL_USERNAME,
            sasl_password=_TEST_SASL_SECRET,
            ssl_ca_location=_TEST_CA_LOCATION,
        ),
        extra={"compression.type": "zstd", "linger.ms": 10, "enable.idempotence": True},
    )

    config = settings.to_confluent_config()

    assert config["bootstrap.servers"] == "k1:9092,k2:9092"
    assert config["client.id"] == "producer-a"
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "PLAIN"
    assert config["sasl.username"] == _TEST_SASL_USERNAME
    assert config["sasl.password"] == _TEST_SASL_SECRET
    assert config["ssl.ca.location"] == _TEST_CA_LOCATION
    assert config["compression.type"] == "zstd"
    assert config["linger.ms"] == 10
    assert config["enable.idempotence"] is True


def test_consumer_settings_compile_to_confluent_config() -> None:
    settings = ConsumerSettings(
        brokers=("k1:9092",),
        group_id="g-1",
        topics=("orders",),
        auto_offset_reset="latest",
        delivery="at_least_once",
        extra={"fetch.min.bytes": 1024},
    )

    config = settings.to_confluent_config()

    assert config["bootstrap.servers"] == "k1:9092"
    assert config["group.id"] == "g-1"
    assert config["auto.offset.reset"] == "latest"
    assert config["fetch.min.bytes"] == 1024


_CONSUMER_SECTION: dict[str, object] = {
    "brokers": ["k1:9092"],
    "group_id": "g-1",
    "topics": ["orders"],
}


def test_consumer_settings_require_delivery() -> None:
    with pytest.raises(msgspec.ValidationError, match="missing required field `delivery`"):
        msgspec.convert(_CONSUMER_SECTION, ConsumerSettings)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [("poll_timeout_ms", 100), ("enable_auto_commit", False)],
)
def test_consumer_settings_refuse_removed_fields(field_name: str, value: object) -> None:
    section = {**_CONSUMER_SECTION, "delivery": "at_least_once", field_name: value}

    with pytest.raises(msgspec.ValidationError, match=f"unknown field `{field_name}`"):
        msgspec.convert(section, ConsumerSettings)


@pytest.mark.parametrize(
    ("delivery", "expected_auto_commit"),
    [
        ("at_least_once", False),
        ("at_most_once", True),
    ],
)
def test_consumer_settings_confluent_auto_commit_derives_from_delivery(
    delivery: str,
    expected_auto_commit: bool,
) -> None:
    settings = ConsumerSettings(
        brokers=("k1:9092",),
        group_id="g-1",
        topics=("orders",),
        delivery=delivery,  # type: ignore[arg-type]
    )

    assert settings.to_confluent_config()["enable.auto.commit"] is expected_auto_commit


def test_consumer_settings_batching_defaults() -> None:
    settings = ConsumerSettings(
        brokers=("k1:9092",),
        group_id="g-1",
        topics=("orders",),
        delivery="at_most_once",
    )

    assert settings.batch_size == 500
    assert settings.poll_backoff_ms == 50


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("batch_size", 0),
        ("batch_size", -1),
        ("poll_backoff_ms", 0),
        ("poll_backoff_ms", -1),
    ],
)
def test_consumer_settings_reject_non_positive_batching_values(
    field_name: str,
    value: int,
) -> None:
    with pytest.raises(ValueError, match=field_name):
        ConsumerSettings(
            brokers=("k1:9092",),
            group_id="g-1",
            topics=("orders",),
            delivery="at_most_once",
            **{field_name: value},  # type: ignore[arg-type]
        )


def test_security_settings_omit_unset_optional_values() -> None:
    settings = ProducerSettings(
        brokers=("k1:9092",),
        security=KafkaSecuritySettings(protocol="SSL"),
    )

    config = settings.to_confluent_config()

    assert config["security.protocol"] == "SSL"
    assert "sasl.mechanism" not in config
    assert "sasl.username" not in config
    assert "sasl.password" not in config
    assert "ssl.ca.location" not in config


def test_producer_settings_reject_extra_override_of_typed_keys() -> None:
    settings = ProducerSettings(
        brokers=("k1:9092",),
        extra={"bootstrap.servers": "other:9092"},
    )

    with pytest.raises(ValueError, match="bootstrap.servers"):
        settings.to_confluent_config()


def test_producer_settings_reject_extra_override_of_security_keys() -> None:
    settings = ProducerSettings(
        brokers=("k1:9092",),
        security=KafkaSecuritySettings(protocol="SSL"),
        extra={"security.protocol": "PLAINTEXT"},
    )

    with pytest.raises(ValueError, match="security.protocol"):
        settings.to_confluent_config()


def test_consumer_settings_reject_extra_override_of_typed_keys() -> None:
    settings = ConsumerSettings(
        brokers=("k1:9092",),
        group_id="g-1",
        topics=("orders",),
        extra={"group.id": "other-group"},
        delivery="at_most_once",
    )

    with pytest.raises(ValueError, match="group.id"):
        settings.to_confluent_config()


def test_kafka_settings_resolve_specific_config_before_default() -> None:
    default_producer = ProducerSettings(brokers=("default:9092",))
    specific_producer = ProducerSettings(
        brokers=("writer:9092",),
        topic="orders.validated.v1",
    )
    default_consumer = ConsumerSettings(
        brokers=("default:9092",),
        group_id="default",
        topics=("default.topic",),
        delivery="at_most_once",
    )
    specific_consumer = ConsumerSettings(
        brokers=("reader:9092",),
        group_id="orders",
        topics=("orders.raw.v1",),
        delivery="at_most_once",
    )
    settings = KafkaSettings(
        producer=default_producer,
        consumer=default_consumer,
        producers={"validated-orders": specific_producer},
        consumers={"orders-input": specific_consumer},
    )

    assert settings.producer_for(LogicalRef("validated-orders")) is specific_producer
    assert settings.producer_for("unknown-output") is default_producer
    assert settings.consumer_for(LogicalRef("orders-input")) is specific_consumer
    assert settings.consumer_for("unknown-input") is default_consumer


def test_kafka_settings_raise_when_no_specific_or_default_config_exists() -> None:
    settings = KafkaSettings()

    with pytest.raises(KeyError, match="missing-output"):
        settings.producer_for("missing-output")

    with pytest.raises(KeyError, match="missing-input"):
        settings.consumer_for("missing-input")


def test_topic_resolution_uses_configured_physical_topic_before_boundary_fallback() -> None:
    producer = ProducerSettings(brokers=("k1:9092",), topic="physical.out")
    consumer = ConsumerSettings(
        brokers=("k1:9092",),
        group_id="g-1",
        topics=("physical.in",),
        delivery="at_most_once",
    )

    assert resolve_producer_topic(LogicalRef("logical-output"), producer) == "physical.out"
    assert resolve_consumer_topics(LogicalRef("logical-input"), consumer) == ("physical.in",)
    assert (
        resolve_producer_topic(
            LogicalRef("logical-output"),
            ProducerSettings(brokers=("k1:9092",)),
        )
        == "logical-output"
    )


def test_commit_keepalive_ms_rejects_non_positive_values() -> None:
    with pytest.raises(ValueError, match="commit_keepalive_ms"):
        ConsumerSettings(
            brokers=("localhost:9092",),
            group_id="test",
            topics=("orders.in",),
            commit_keepalive_ms=0,
            delivery="at_most_once",
        )
