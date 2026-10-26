"""Shared fixtures for Bytewax unit tests."""

from __future__ import annotations

import pytest

from loom.streaming import FromTopic, IntoTopic, Process, StreamFlow
from loom.streaming.core._message import Message
from tests.unit.streaming.bytewax.cases import (
    DoubleStep,
    Order,
    Result,
    SuffixStep,
    build_message,
)


@pytest.fixture
def bytewax_order() -> Order:
    """Return the canonical Bytewax order payload."""

    return Order(order_id="A")


@pytest.fixture
def bytewax_result() -> Result:
    """Return the canonical Bytewax result payload."""

    return Result(value="AA")


@pytest.fixture
def bytewax_message(bytewax_order: Order) -> Message[Order]:
    """Return the canonical Bytewax message."""

    return build_message(bytewax_order)


@pytest.fixture
def bytewax_double_step() -> DoubleStep:
    """Return the canonical first record step."""

    return DoubleStep()


@pytest.fixture
def bytewax_suffix_step() -> SuffixStep:
    """Return the canonical second record step."""

    return SuffixStep()


@pytest.fixture
def bytewax_runtime_config_dict() -> dict[str, object]:
    """Return the canonical runtime configuration for Bytewax tests."""

    return {
        "kafka": {
            "consumer": {
                "brokers": ["localhost:9092"],
                "group_id": "test",
                "delivery": "at_most_once",
                "topics": ["orders.in"],
            },
            "producer": {
                "brokers": ["localhost:9092"],
                "client_id": "test-producer",
                "topic": "orders.out",
            },
        },
        "streaming": {
            "runtime": {
                "workers_per_process": 2,
                "epoch_interval_ms": 5000,
                "addresses": ["127.0.0.1:2101", "127.0.0.1:2102"],
                "process_id": 1,
                "recovery": {
                    "db_dir": "/var/lib/loom/tests/bytewax-recovery",
                    "backup_interval_ms": 30000,
                },
            },
        },
        "observability": {
            "log": {"enabled": False},
            "otel": {
                "enabled": True,
                "config": {
                    "service_name": "loom-streaming",
                    "tracer_name": "loom.streaming",
                    "endpoint": "",
                },
            },
        },
    }


@pytest.fixture
def bytewax_stream_flow() -> StreamFlow[Order, Result]:
    """Return a canonical Bytewax flow used by runner tests."""

    return StreamFlow(
        name="runner_flow",
        source=FromTopic("orders.in", payload=Order),
        process=Process(DoubleStep()),
        output=IntoTopic("orders.out", payload=Result),
    )
