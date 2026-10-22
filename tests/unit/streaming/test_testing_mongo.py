"""Streaming test-runner coverage for MongoDB CDC sources."""

from __future__ import annotations

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.model import LoomStruct  # noqa: E402
from loom.streaming import (  # noqa: E402
    FromMongoCDC,
    IntoTopic,
    Message,
    Process,
    RecordStep,
    StreamFlow,
)
from loom.streaming.mongo import MongoBsonTimestamp, MongoCDCEvent, MongoCDCNamespace  # noqa: E402
from loom.streaming.testing import StreamingTestRunner  # noqa: E402

pytestmark = pytest.mark.bytewax


class _Envelope(LoomStruct):
    event_id: str
    operation_type: str


class _MapMongoEvent(RecordStep[MongoCDCEvent, _Envelope]):
    def execute(self, message: Message[MongoCDCEvent], **kwargs: object) -> _Envelope:
        del kwargs
        return _Envelope(
            event_id=message.payload.event_id,
            operation_type=message.payload.operation_type,
        )


def test_streaming_test_runner_accepts_mongo_source_payloads() -> None:
    flow: StreamFlow[MongoCDCEvent, _Envelope] = StreamFlow(
        name="mongo_runner_flow",
        source=FromMongoCDC("domain_events", collections=("orders",)),
        process=Process(_MapMongoEvent()),
        output=IntoTopic("orders.out", payload=_Envelope),
    )
    runner = StreamingTestRunner.from_dict(flow, _mongo_runner_config())

    runner.with_payloads([_mongo_event("evt-1")]).run()

    assert len(runner.output) == 1
    result = runner.output[0]
    assert isinstance(result, Message)
    assert result.payload == _Envelope(event_id="evt-1", operation_type="insert")


def _mongo_event(event_id: str) -> MongoCDCEvent:
    return MongoCDCEvent(
        event_id=event_id,
        operation_type="insert",
        namespace=MongoCDCNamespace(db="app", coll="orders"),
        resume_token={"token": event_id},
        document_id="order-1",
        cluster_time=MongoBsonTimestamp(seconds=1, increment=2),
        wall_time_ms=1000,
        full_document={"status": "created"},
        update_description=None,
        raw_json='{"operationType":"insert"}',
    )


def _mongo_runner_config() -> dict[str, object]:
    return {
        "mongo": {
            "sources": {
                "domain_events": {
                    "url": "mongodb://localhost:27017",
                    "database": "app",
                }
            }
        },
        "kafka": {
            "producer": {
                "brokers": ["localhost:9092"],
                "topic": "orders.out",
            },
            "producers": {
                "orders.out": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.out",
                }
            },
        },
    }
