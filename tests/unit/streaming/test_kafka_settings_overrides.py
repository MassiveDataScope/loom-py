"""``load_kafka_settings`` loads an application's Kafka section, overrides included."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from loom.streaming.kafka._config import load_kafka_settings


def test_a_loom_override_reaches_the_kafka_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "kafka.yaml"
    document = {
        "kafka": {
            "consumer": {"brokers": ["a:9092"], "group_id": "g", "topics": ["t"]},
            "producer": {"brokers": ["a:9092"], "client_id": "c", "topic": "o"},
        }
    }
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    monkeypatch.setenv("LOOM__KAFKA__CONSUMER__GROUP_ID", "overridden")

    consumer = load_kafka_settings(str(path)).consumer
    assert consumer is not None
    assert consumer.group_id == "overridden"
