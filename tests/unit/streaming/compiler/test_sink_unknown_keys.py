"""A table sink section with a key its backend does not read, or two sources of one
setting, fails at compile time."""

from __future__ import annotations

from typing import Any, cast

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.core.config import ConfigContext
from loom.streaming import FromTopic, IntoTable, Process, StreamFlow, TableEngine
from loom.streaming.compiler import CompilationError, StreamingErrorCode, compile_flow

from .cases import Order, Result

_SINK = "results_sink"

_SECTIONS: dict[TableEngine, dict[str, object]] = {
    TableEngine.DELTA: {"uri": "s3://lake/results"},
    TableEngine.SQLALCHEMY: {"url": "sqlite+aiosqlite:///:memory:"},
    TableEngine.CLICKHOUSE: {"store": "clickhouse"},
}


def _flow(backend: TableEngine) -> StreamFlow[Order, Result]:
    sink = IntoTable(payload=Result, table="results", engine=backend, name=_SINK)
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
                "stores": {
                    "clickhouse": {
                        "backend": "clickhouse",
                        "url": "clickhouse://localhost:8123/default",
                    },
                    "warehouse": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///:memory:"},
                },
                "streaming": {"sinks": {_SINK: section}},
            }
        ),
    )
    return ConfigContext(cast(DictConfig, merged))


class TestSinkSectionKeys:
    @pytest.mark.parametrize("backend", list(_SECTIONS))
    def test_a_known_section_compiles(
        self, streaming_kafka_config: DictConfig, backend: TableEngine
    ) -> None:
        plan = compile_flow(
            _flow(backend), config=_config(streaming_kafka_config, _SECTIONS[backend])
        )

        assert plan.name == "sink_flow"

    @pytest.mark.parametrize("backend", list(_SECTIONS))
    def test_an_unknown_key_is_a_sink_config_issue_naming_the_key(
        self, streaming_kafka_config: DictConfig, backend: TableEngine
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

    @pytest.mark.parametrize(
        ("backend", "key", "database", "remedy"),
        [
            (
                TableEngine.SQLALCHEMY,
                "store",
                "warehouse",
                "set 'store' for a stores.<name> section or 'url' inline, not both",
            ),
            (
                TableEngine.CLICKHOUSE,
                "store",
                "clickhouse",
                "the url comes from stores.<name>, so set 'store' only",
            ),
        ],
    )
    def test_a_section_with_both_url_and_database_is_refused(
        self,
        streaming_kafka_config: DictConfig,
        backend: TableEngine,
        key: str,
        database: str,
        remedy: str,
    ) -> None:
        section = {"url": "sqlite+aiosqlite:///:memory:", key: database}
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
        assert f"both 'url' and '{key}'" in issues[0].message
        assert remedy in issues[0].message

    @pytest.mark.parametrize("key", ["staging", "writer_properties"])
    def test_a_delta_key_that_is_not_a_mapping_is_a_sink_config_issue(
        self, streaming_kafka_config: DictConfig, key: str
    ) -> None:
        section = {**_SECTIONS[TableEngine.DELTA], key: "zstd"}
        flow = _flow(TableEngine.DELTA)
        context = _config(streaming_kafka_config, section)

        with pytest.raises(CompilationError) as failure:
            compile_flow(flow, config=context)

        issues = [
            issue
            for issue in failure.value.issues
            if issue.code is StreamingErrorCode.SINK_CONFIG_INVALID
        ]
        assert len(issues) == 1
        assert f"'{key}' must be a mapping" in issues[0].message
