"""Tests for the partitioned runtime Kafka source in the Bytewax adapter."""

from __future__ import annotations

import logging
import time
from types import SimpleNamespace

import pytest
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, KafkaError, TopicPartition

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.streaming.bytewax import _runtime_io  # noqa: E402
from loom.streaming.bytewax._commit_tracker import KafkaCommitTracker  # noqa: E402
from loom.streaming.kafka._errors import KafkaCommitError, KafkaPollError  # noqa: E402
from loom.streaming.kafka._record import KafkaRecord  # noqa: E402
from tests.unit.streaming.bytewax.cases import build_compiled_source  # noqa: E402
from tests.unit.streaming.kafka.fakes import (  # noqa: E402
    PartitionClientInstaller,
    PartitionClientStub,
)


class _MetadataError:
    """Stand-in for the KafkaError that confluent reports inside topic metadata."""

    def __init__(self, code: int) -> None:
        self._code = code

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return f"metadata error {self._code}"


def _kafka_error(code: int) -> _MetadataError:
    return _MetadataError(code)


def _shorten_metadata_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """Collapse the metadata visibility window so retries do not slow the suite."""
    monkeypatch.setattr(_runtime_io, "_METADATA_VISIBILITY_TIMEOUT_S", 0.05)
    monkeypatch.setattr(_runtime_io, "_METADATA_RETRY_INTERVAL_S", 0.001)


pytestmark = pytest.mark.bytewax


def _record(offset: int, *, partition: int = 2, topic: str = "orders.in") -> KafkaRecord[bytes]:
    return KafkaRecord(topic=topic, key=None, value=b"raw", partition=partition, offset=offset)


def _install_clients(
    monkeypatch: pytest.MonkeyPatch,
    *stubs: PartitionClientStub,
) -> PartitionClientInstaller:
    installer = PartitionClientInstaller(list(stubs))
    monkeypatch.setattr(_runtime_io, "KafkaConsumerClient", installer)
    return installer


class TestKafkaPartitionedSourceBuildPart:
    def test_resume_state_wins_over_committed_offset(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stub = PartitionClientStub()
        stub.committed[("orders.in", 2)] = 40
        _install_clients(monkeypatch, stub)
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())

        source.build_part("s", "orders.in:2", resume_state=55)

        assert stub.assign_calls == [("orders.in", 2, 55)]

    def test_committed_offset_wins_over_reset_policy(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stub = PartitionClientStub()
        stub.committed[("orders.in", 2)] = 40
        _install_clients(monkeypatch, stub)
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())

        source.build_part("s", "orders.in:2", resume_state=None)

        assert stub.assign_calls == [("orders.in", 2, 40)]
        assert stub.committed_requests == [
            ("orders.in", 2, _runtime_io._COMMITTED_FETCH_TIMEOUT_MS)
        ]

    def test_reset_policy_maps_to_confluent_sentinels(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        earliest = PartitionClientStub()
        latest = PartitionClientStub()
        _install_clients(monkeypatch, earliest, latest)
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())
        source.build_part("s", "orders.in:0", resume_state=None)
        assert earliest.assign_calls == [("orders.in", 0, int(OFFSET_BEGINNING))]

        latest_source = _runtime_io.KafkaPartitionedSource(
            build_compiled_source(auto_offset_reset="latest")
        )
        latest_source.build_part("s", "orders.in:0", resume_state=None)
        assert latest.assign_calls == [("orders.in", 0, int(OFFSET_END))]

    def test_rejects_partition_key_outside_flow_topics(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install_clients(monkeypatch, PartitionClientStub())
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())

        with pytest.raises(ValueError, match="does not belong"):
            source.build_part("s", "other.topic:0", resume_state=None)

    def test_sets_floor_and_binds_partition_committer(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stub = PartitionClientStub()
        stub.committed[("orders.in", 2)] = 10
        _install_clients(monkeypatch, stub)
        tracker = KafkaCommitTracker()
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source(), tracker)
        partition = source.build_part("s", "orders.in:2", resume_state=None)

        for offset in (10, 11):
            tracker.register_record(_record(offset))
            tracker.complete("orders.in", 2, offset)
        stub.batches = [[]]
        partition.next_batch()

        # watermark 12 > floor 10: committed through the partition's own client
        assert stub.commit_offset_calls == [[TopicPartition("orders.in", 2, 12)]]

    def test_warns_when_resume_state_is_behind_committed(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        stub = PartitionClientStub()
        stub.committed[("orders.in", 2)] = 100
        _install_clients(monkeypatch, stub)
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())

        with caplog.at_level(logging.WARNING):
            source.build_part("s", "orders.in:2", resume_state=60)

        assert any("behind the committed group offset" in r.message for r in caplog.records)


class TestKafkaSourcePartition:
    def test_registers_records_before_returning_and_snapshots_next_offset(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stub = PartitionClientStub()
        stub.batches = [[_record(7), _record(8)]]
        _install_clients(monkeypatch, stub)
        tracker = KafkaCommitTracker()
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source(), tracker)
        partition = source.build_part("s", "orders.in:2", resume_state=None)

        records = partition.next_batch()

        assert [r.offset for r in records] == [7, 8]
        assert partition.snapshot() == 9
        assert partition.next_awake() is None
        # registered before returning: completing both now advances the watermark
        tracker.complete("orders.in", 2, 7)
        tracker.complete("orders.in", 2, 8)
        assert tracker.flush_partition("orders.in", 2) == [TopicPartition("orders.in", 2, 9)]

    def test_empty_batch_backs_off_and_flushes_pending_commits(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stub = PartitionClientStub()
        stub.batches = [[_record(3)], []]
        _install_clients(monkeypatch, stub)
        tracker = KafkaCommitTracker()
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source(), tracker)
        partition = source.build_part("s", "orders.in:2", resume_state=None)

        partition.next_batch()
        tracker.complete("orders.in", 2, 3)  # completes on a sink thread
        partition.next_batch()  # empty poll: coalesced flush happens here

        assert stub.commit_offset_calls == [[TopicPartition("orders.in", 2, 4)]]
        assert partition.next_awake() is not None
        assert partition.snapshot() == 4

    def test_close_flushes_final_watermark_and_closes_client(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stub = PartitionClientStub()
        stub.batches = [[_record(5)]]
        _install_clients(monkeypatch, stub)
        tracker = KafkaCommitTracker()
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source(), tracker)
        partition = source.build_part("s", "orders.in:2", resume_state=None)
        partition.next_batch()
        tracker.complete("orders.in", 2, 5)

        partition.close()

        assert stub.commit_offset_calls == [[TopicPartition("orders.in", 2, 6)]]
        assert stub.closed is True

    def test_close_releases_the_client_even_when_the_final_flush_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A broker down at shutdown must not leak the consumer.

        The closing commit is synchronous and re-raises by design, so without a
        try/finally the client survives the exception together with its sockets
        and group state.
        """
        stub = PartitionClientStub()
        stub.batches = [[_record(5)]]
        stub.commit_error = KafkaCommitError("broker down at shutdown")
        _install_clients(monkeypatch, stub)
        tracker = KafkaCommitTracker()
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source(), tracker)
        partition = source.build_part("s", "orders.in:2", resume_state=None)
        partition.next_batch()
        tracker.complete("orders.in", 2, 5)

        with pytest.raises(KafkaCommitError, match="broker down at shutdown"):
            partition.close()

        assert stub.closed is True, "the consumer leaked when the final flush raised"


class TestListParts:
    def test_lists_one_key_per_kafka_partition(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _TopicMeta:
            error = None
            partitions = {1: object(), 0: object()}

        class _Metadata:
            topics = {"orders.in": _TopicMeta()}

        class _Admin:
            def __init__(self, config: dict[str, object]) -> None:
                self.config = config

            def list_topics(self, topic: str, timeout: float) -> _Metadata:
                del topic, timeout
                return _Metadata()

        monkeypatch.setattr(_runtime_io, "AdminClient", _Admin)
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())

        assert source.list_parts() == ["orders.in:0", "orders.in:1"]

    def test_metadata_failure_is_a_hard_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _Metadata:
            topics: dict[str, object] = {}

        class _Admin:
            def __init__(self, config: dict[str, object]) -> None:
                del config

            def list_topics(self, topic: str, timeout: float) -> _Metadata:
                del topic, timeout
                return _Metadata()

        _shorten_metadata_wait(monkeypatch)
        monkeypatch.setattr(_runtime_io, "AdminClient", _Admin)
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())

        with pytest.raises(KafkaPollError, match="cannot list partitions"):
            source.list_parts()

    def test_a_topic_that_appears_late_is_awaited_instead_of_failing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _TopicMeta:
            error = None
            partitions = {0: object(), 1: object()}

        class _Admin:
            fetches = 0

            def __init__(self, config: dict[str, object]) -> None:
                del config

            def list_topics(self, topic: str, timeout: float) -> object:
                del timeout
                type(self).fetches += 1
                if type(self).fetches < 3:
                    return SimpleNamespace(topics={})
                return SimpleNamespace(topics={topic: _TopicMeta()})

        _shorten_metadata_wait(monkeypatch)
        monkeypatch.setattr(_runtime_io, "AdminClient", _Admin)
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())

        assert source.list_parts() == ["orders.in:0", "orders.in:1"]
        assert _Admin.fetches == 3

    def test_a_leaderless_partition_is_awaited_until_a_leader_is_elected(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _Admin:
            fetches = 0

            def __init__(self, config: dict[str, object]) -> None:
                del config

            def list_topics(self, topic: str, timeout: float) -> object:
                del timeout
                type(self).fetches += 1
                if type(self).fetches == 1:
                    meta = SimpleNamespace(
                        error=_kafka_error(KafkaError.LEADER_NOT_AVAILABLE), partitions={}
                    )
                else:
                    meta = SimpleNamespace(error=None, partitions={0: object()})
                return SimpleNamespace(topics={topic: meta})

        _shorten_metadata_wait(monkeypatch)
        monkeypatch.setattr(_runtime_io, "AdminClient", _Admin)
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())

        assert source.list_parts() == ["orders.in:0"]

    def test_an_error_no_wait_can_resolve_fails_on_the_first_fetch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _Admin:
            fetches = 0

            def __init__(self, config: dict[str, object]) -> None:
                del config

            def list_topics(self, topic: str, timeout: float) -> object:
                del timeout
                type(self).fetches += 1
                meta = SimpleNamespace(
                    error=_kafka_error(KafkaError.TOPIC_AUTHORIZATION_FAILED), partitions={}
                )
                return SimpleNamespace(topics={topic: meta})

        _shorten_metadata_wait(monkeypatch)
        monkeypatch.setattr(_runtime_io, "AdminClient", _Admin)
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())

        with pytest.raises(KafkaPollError, match="cannot list partitions"):
            source.list_parts()

        assert _Admin.fetches == 1


class TestSnapshotSeeding:
    def test_snapshot_returns_resume_state_before_any_batch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install_clients(monkeypatch, PartitionClientStub())
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())

        partition = source.build_part("s", "orders.in:2", resume_state=55)

        assert partition.snapshot() == 55

    def test_snapshot_falls_back_to_committed_then_none_for_sentinels(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with_committed = PartitionClientStub()
        with_committed.committed[("orders.in", 2)] = 40
        fresh = PartitionClientStub()
        _install_clients(monkeypatch, with_committed, fresh)
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source())

        seeded = source.build_part("s", "orders.in:2", resume_state=None)
        sentinel = source.build_part("s", "orders.in:3", resume_state=None)

        assert seeded.snapshot() == 40
        assert sentinel.snapshot() is None


class TestKeepAlive:
    def test_idle_partition_recommits_committed_offset(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retention keep-alive: a partition with zero traffic re-commits its floor."""
        stub = PartitionClientStub()
        stub.committed[("orders.in", 2)] = 10
        _install_clients(monkeypatch, stub)
        tracker = KafkaCommitTracker()
        source = _runtime_io.KafkaPartitionedSource(
            build_compiled_source(commit_keepalive_ms=1), tracker
        )
        partition = source.build_part("s", "orders.in:2", resume_state=None)

        time.sleep(0.003)
        partition.next_batch()  # empty poll past the keep-alive window

        assert stub.commit_offset_calls == [[TopicPartition("orders.in", 2, 10)]]

    def test_cycle_flush_is_asynchronous_and_close_is_synchronous(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stub = PartitionClientStub()
        stub.batches = [[_record(7)], []]
        _install_clients(monkeypatch, stub)
        tracker = KafkaCommitTracker()
        source = _runtime_io.KafkaPartitionedSource(build_compiled_source(), tracker)
        partition = source.build_part("s", "orders.in:2", resume_state=None)

        partition.next_batch()
        tracker.complete("orders.in", 2, 7)
        partition.next_batch()  # cycle flush: async
        partition.close()  # final flush: sync (force re-commits the watermark)

        assert stub.commit_async_flags == [True, False]


class TestObservabilityPassthrough:
    def test_partition_clients_receive_the_observability_runtime(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = _install_clients(monkeypatch, PartitionClientStub())
        observability = object()
        source = _runtime_io.build_runtime_source(build_compiled_source(), None, observability)

        assert isinstance(source, _runtime_io.KafkaPartitionedSource)
        source.build_part("s", "orders.in:2", resume_state=None)

        assert installer.unassigned_calls[0][1] is observability


class TestListPartsMultiTopic:
    def test_lists_partitions_of_every_source_topic(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _TopicMeta:
            error = None
            partitions = {0: object()}

        class _Metadata:
            def __init__(self, topic: str) -> None:
                self.topics = {topic: _TopicMeta()}

        class _Admin:
            def __init__(self, config: dict[str, object]) -> None:
                del config

            def list_topics(self, topic: str, timeout: float) -> _Metadata:
                del timeout
                return _Metadata(topic)

        monkeypatch.setattr(_runtime_io, "AdminClient", _Admin)
        source = _runtime_io.KafkaPartitionedSource(
            build_compiled_source(topics=("orders.in", "audit.in"))
        )

        assert source.list_parts() == ["orders.in:0", "audit.in:0"]
