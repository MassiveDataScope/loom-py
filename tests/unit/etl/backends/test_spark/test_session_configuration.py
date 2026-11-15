"""Tests for the configuration SparkTestSession applies to a local session."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

pytest.importorskip("pyspark")
pytest.importorskip("delta")


class TestSparkSessionConfiguration:
    def test_snapshot_partitions_default_keeps_delta_log_replay_single_tasked(
        self,
        spark: SparkSession,
    ) -> None:
        assert spark.conf.get("spark.databricks.delta.snapshotPartitions") == "1"

    def test_shuffle_partitions_follow_the_requested_parallelism(self, spark: SparkSession) -> None:
        assert spark.conf.get("spark.sql.shuffle.partitions") == "1"
