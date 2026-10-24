"""Compiler tests for MongoDB CDC sources."""

from __future__ import annotations

import pytest
from omegaconf import OmegaConf

from loom.core.model.redaction import Dsn, RedactedMap
from loom.streaming import Drain, FromMongoCDC, MongoSourceConfig, Process, StreamFlow, compile_flow
from loom.streaming.compiler import CompilationError, CompiledMongoCDCSource
from loom.streaming.mongo import MongoCDCEvent


def test_compile_success_with_mongo_source() -> None:
    flow: StreamFlow[MongoCDCEvent, MongoCDCEvent] = StreamFlow(
        name="mongo_flow",
        source=FromMongoCDC(
            "domain_events",
            collections=("orders",),
            watch_options={"full_document": "updateLookup"},
        ),
        process=Process(Drain()),
    )

    plan = compile_flow(
        flow,
        config=OmegaConf.create(
            {
                "mongo": {
                    "sources": {
                        "domain_events": {
                            "url": "mongodb://localhost:27017",
                            "database": "app",
                            "watch_options": {"max_await_time_ms": 1000},
                        }
                    }
                }
            }
        ),
    )

    assert isinstance(plan.source, CompiledMongoCDCSource)
    assert plan.source.settings == MongoSourceConfig(
        url=Dsn("mongodb://localhost:27017"),
        database="app",
        watch_options=RedactedMap({"max_await_time_ms": 1000}),
    )
    assert plan.source.collections == ("orders",)
    assert plan.source.watch_options == {
        "max_await_time_ms": 1000,
        "full_document": "updateLookup",
    }


def test_compile_fails_when_mongo_config_is_missing() -> None:
    flow: StreamFlow[MongoCDCEvent, MongoCDCEvent] = StreamFlow(
        name="mongo_flow",
        source=FromMongoCDC("domain_events"),
        process=Process(Drain()),
    )

    config = OmegaConf.create({})
    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, config=config)

    assert "mongo source 'domain_events'" in str(exc_info.value)
