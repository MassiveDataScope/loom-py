from __future__ import annotations

from pathlib import Path

import pytest

from loom.streaming.kafka import KafkaSettings, load_kafka_settings

pytestmark = pytest.mark.kafka


def test_load_kafka_settings_uses_core_config_section(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text(
        """
kafka:
  producer:
    brokers:
      - localhost:9092
    client_id: orders-producer
    extra:
      linger.ms: 5
  consumer:
    brokers:
      - localhost:9092
    group_id: orders
    delivery: at_most_once
    topics:
      - orders.in
    auto_offset_reset: latest
""".lstrip(),
        encoding="utf-8",
    )

    settings = load_kafka_settings(str(path))

    assert isinstance(settings, KafkaSettings)
    assert settings.producer is not None
    assert settings.producer.brokers == ("localhost:9092",)
    assert settings.producer.client_id == "orders-producer"
    assert settings.producer.extra == {"linger.ms": 5}
    assert settings.consumer is not None
    assert settings.consumer.group_id == "orders"
    assert settings.consumer.topics == ("orders.in",)
    assert settings.consumer.auto_offset_reset == "latest"
    assert settings.producers == {}
    assert settings.consumers == {}


def test_load_kafka_settings_allows_nested_section_name(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text(
        """
streaming:
  kafka:
    producer:
      brokers:
        - kafka:9092
""".lstrip(),
        encoding="utf-8",
    )

    settings = load_kafka_settings(str(path), section_name="streaming.kafka")

    assert settings.producer is not None
    assert settings.producer.brokers == ("kafka:9092",)
    assert settings.consumer is None


def test_load_kafka_settings_supports_named_overrides(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text(
        """
kafka:
  producer:
    brokers:
      - default:9092
  consumer:
    brokers:
      - default:9092
    group_id: default-group
    delivery: at_most_once
    topics:
      - default.topic

  producers:
    validated-orders:
      brokers:
        - writer:9092
      client_id: validated-writer
      topic: orders.validated.v1

  consumers:
    orders-input:
      brokers:
        - reader:9092
      group_id: orders-reader
      delivery: at_most_once
      topics:
        - orders.raw.v1
""".lstrip(),
        encoding="utf-8",
    )

    settings = load_kafka_settings(str(path))

    assert settings.producer_for("missing").brokers == ("default:9092",)
    assert settings.consumer_for("missing").group_id == "default-group"
    assert settings.producer_for("validated-orders").brokers == ("writer:9092",)
    assert settings.producer_for("validated-orders").topic == "orders.validated.v1"
    assert settings.consumer_for("orders-input").brokers == ("reader:9092",)
    assert settings.consumer_for("orders-input").topics == ("orders.raw.v1",)
