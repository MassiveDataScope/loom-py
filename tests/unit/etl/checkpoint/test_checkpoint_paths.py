"""Unit tests for checkpoint path helpers."""

from __future__ import annotations

import pytest

from loom.etl.checkpoint._paths import (
    correlation_scope_base,
    run_scope_base,
    runs_root,
    scope_base,
)
from loom.etl.checkpoint._scope import CheckpointScope


class TestCheckpointScopePaths:
    def test_run_scope_base(self) -> None:
        assert run_scope_base("s3://bucket/tmp", "run-1") == "s3://bucket/tmp/runs/run-1"

    def test_correlation_scope_base(self) -> None:
        assert (
            correlation_scope_base("s3://bucket/tmp", "job-1")
            == "s3://bucket/tmp/correlations/job-1"
        )

    def test_scope_base_run(self) -> None:
        assert (
            scope_base(
                "s3://bucket/tmp",
                run_id="run-1",
                correlation_id=None,
                scope=CheckpointScope.RUN,
            )
            == "s3://bucket/tmp/runs/run-1"
        )

    def test_scope_base_correlation(self) -> None:
        assert (
            scope_base(
                "s3://bucket/tmp",
                run_id="run-1",
                correlation_id="job-1",
                scope=CheckpointScope.CORRELATION,
            )
            == "s3://bucket/tmp/correlations/job-1"
        )

    def test_scope_base_correlation_requires_correlation_id(self) -> None:
        with pytest.raises(RuntimeError, match="scope=CORRELATION"):
            scope_base(
                "s3://bucket/tmp",
                run_id="run-1",
                correlation_id=None,
                scope=CheckpointScope.CORRELATION,
            )

    def test_runs_root(self) -> None:
        assert runs_root("s3://bucket/tmp") == "s3://bucket/tmp/runs"
