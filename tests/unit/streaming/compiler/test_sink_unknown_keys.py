"""A table sink section with a key its backend does not read fails at compile time."""

from __future__ import annotations

from typing import Any, cast

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.core.config import ConfigContext
from loom.streaming import Backend, FromTopic, IntoTable, Process, StreamFlow
from loom.streaming.compiler import CompilationError, StreamingErrorCode, compile_flow

from .cases import Order, Result

_SINK = "results_sink"

_SECTIONS: dict[Backend, dict[str, object]] = {
    Backend.DELTA: {"uri": "s3://lake/results"},
    Backend.SQLALCHEMY: {"url": "sqlite+aiosqlite:///:memory:"},
    Backend.CLICKHOUSE: {"database": "clickhouse"},
}


def _flow(backend: Backend) -> StreamFlow[Order, Result]:
    sink = IntoTable(payload=Result, table="results", backend=backend, name=_SINK)
    return StreamFlow(
        name="sink_flow",
        source=FromTopic("in", payload=Order),
        process=Process(cast(Any, sink)),
    )


def _config(base: DictConfig, section: dict[str, object]) -> ConfigContext:
    merged = OmegaConf.merge(
        base,
        OmegaConf.create(
            {
                "database": {"clickhouse": {"url": "clickhouse://localhost:8123/default"}},
                "streaming": {"sinks": {_SINK: section}},
            }
        ),
    )
    return ConfigContext(cast(DictConfig, merged))


@pytest.mark.parametrize("backend", list(_SECTIONS))
def test_a_known_section_compiles(streaming_kafka_config: DictConfig, backend: Backend) -> None:
    plan = compile_flow(_flow(backend), config=_config(streaming_kafka_config, _SECTIONS[backend]))

    assert plan.name == "sink_flow"


@pytest.mark.parametrize("backend", list(_SECTIONS))
def test_an_unknown_key_is_a_sink_config_issue_naming_the_key(
    streaming_kafka_config: DictConfig, backend: Backend
) -> None:
    section = {**_SECTIONS[backend], "chunk_sise": 10}
    flow = _flow(backend)
    context = _config(streaming_kafka_config, section)

    with pytest.raises(CompilationError) as failure:
        compile_flow(flow, config=context)

    issues = [
        issue
        for issue in failure.value.issues
        if issue.code is StreamingErrorCode.SINK_CONFIG_INVALID
    ]
    assert len(issues) == 1
    assert "'chunk_sise'" in issues[0].message
