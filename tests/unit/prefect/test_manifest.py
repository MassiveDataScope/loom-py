"""Tests for loom.prefect._manifest.

Verifies:
- StepEntry and RunManifest are frozen msgspec.Struct objects.
- ManifestStore protocol shape (load, save, delete methods).
- completed_steps() returns only SUCCESS steps (not FAILED, not absent).
- mark_step() adds a new step entry to the manifest (returns new manifest).
- mark_step() updates an existing step (replaces the entry).
- mark_step() is idempotent: marking SUCCESS again yields the same step data.
- mark_step() with error stores the error string.
- RunManifest.steps only contains attempted steps; absent = pending.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from loom.etl.lineage._records import RunStatus
from loom.prefect.manifest import (
    ManifestStore,
    RunManifest,
    StepEntry,
    completed_steps,
    mark_step,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


def _manifest(*entries: StepEntry, correlation_id: str = "corr-1") -> RunManifest:
    return RunManifest(
        correlation_id=correlation_id,
        steps=tuple(entries),
        updated_at=_NOW,
    )


def _success(step: str) -> StepEntry:
    return StepEntry(step=step, status=RunStatus.SUCCESS)


def _failed(step: str, error: str = "boom") -> StepEntry:
    return StepEntry(step=step, status=RunStatus.FAILED, error=error)


class TestStepEntry:
    # ---------------------------------------------------------------------------
    # StepEntry tests
    # ---------------------------------------------------------------------------

    def test_success_construction(self) -> None:
        """StepEntry can be built with status=SUCCESS and no error."""
        entry = _success("StepA")
        assert entry.step == "StepA"
        assert entry.status is RunStatus.SUCCESS
        assert entry.error is None

    def test_stores_the_error_on_failure(self) -> None:
        """StepEntry stores the error message on FAILED status."""
        entry = _failed("StepB", "connection refused")
        assert entry.status is RunStatus.FAILED
        assert entry.error == "connection refused"

    def test_is_immutable(self) -> None:
        """StepEntry must be frozen."""
        entry = _success("StepA")
        with pytest.raises((TypeError, AttributeError)):
            entry.step = "Other"  # type: ignore[misc]


class TestRunManifest:
    # ---------------------------------------------------------------------------
    # RunManifest tests
    # ---------------------------------------------------------------------------

    def test_empty_steps(self) -> None:
        """RunManifest can have an empty steps tuple (all pending)."""
        m = _manifest()
        assert m.steps == ()
        assert m.correlation_id == "corr-1"

    def test_is_immutable(self) -> None:
        """RunManifest must be frozen."""
        m = _manifest(_success("StepA"))
        with pytest.raises((TypeError, AttributeError)):
            m.correlation_id = "other"  # type: ignore[misc]


class TestCompletedSteps:
    # ---------------------------------------------------------------------------
    # completed_steps tests
    # ---------------------------------------------------------------------------

    def test_empty_manifest(self) -> None:
        """completed_steps returns empty frozenset for manifest with no entries."""
        result = completed_steps(_manifest())
        assert result == frozenset()

    def test_returns_only_success(self) -> None:
        """completed_steps excludes FAILED steps."""
        m = _manifest(_success("StepA"), _failed("StepB"))
        result = completed_steps(m)
        assert result == frozenset({"StepA"})

    def test_all_success(self) -> None:
        """completed_steps returns all step names when all succeeded."""
        m = _manifest(_success("StepA"), _success("StepB"), _success("StepC"))
        result = completed_steps(m)
        assert result == frozenset({"StepA", "StepB", "StepC"})

    def test_all_failed(self) -> None:
        """completed_steps returns empty frozenset when all steps failed."""
        m = _manifest(_failed("StepA"), _failed("StepB"))
        result = completed_steps(m)
        assert result == frozenset()

    def test_returns_a_frozenset(self) -> None:
        """completed_steps return type is frozenset."""
        result = completed_steps(_manifest(_success("StepA")))
        assert isinstance(result, frozenset)


class TestMarkStep:
    # ---------------------------------------------------------------------------
    # mark_step tests
    # ---------------------------------------------------------------------------

    def test_adds_a_new_step(self) -> None:
        """mark_step adds a new StepEntry when the step is not present."""
        m = _manifest()
        updated = mark_step(m, "StepA", RunStatus.SUCCESS)
        assert len(updated.steps) == 1
        assert updated.steps[0].step == "StepA"
        assert updated.steps[0].status is RunStatus.SUCCESS

    def test_returns_a_new_manifest(self) -> None:
        """mark_step is pure — it returns a new RunManifest, not mutating the original."""
        m = _manifest()
        updated = mark_step(m, "StepA", RunStatus.SUCCESS)
        assert updated is not m
        assert len(m.steps) == 0

    def test_updates_an_existing_step(self) -> None:
        """mark_step replaces an existing StepEntry for the same step name."""
        m = _manifest(_failed("StepA", "first error"))
        updated = mark_step(m, "StepA", RunStatus.SUCCESS)
        step_names = [e.step for e in updated.steps]
        assert step_names.count("StepA") == 1
        entry = next(e for e in updated.steps if e.step == "StepA")
        assert entry.status is RunStatus.SUCCESS

    def test_is_idempotent_for_success(self) -> None:
        """Marking SUCCESS twice for the same step yields the same data."""
        m = _manifest(_success("StepA"))
        updated = mark_step(m, "StepA", RunStatus.SUCCESS)
        entry = next(e for e in updated.steps if e.step == "StepA")
        assert entry.status is RunStatus.SUCCESS
        assert entry.error is None

    def test_stores_the_error_on_failure(self) -> None:
        """mark_step stores the error string when status=FAILED."""
        m = _manifest()
        updated = mark_step(m, "StepA", RunStatus.FAILED, error="timeout")
        entry = next(e for e in updated.steps if e.step == "StepA")
        assert entry.status is RunStatus.FAILED
        assert entry.error == "timeout"

    def test_preserves_other_steps(self) -> None:
        """mark_step does not remove other step entries."""
        m = _manifest(_success("StepA"), _success("StepB"))
        updated = mark_step(m, "StepC", RunStatus.SUCCESS)
        step_names = {e.step for e in updated.steps}
        assert step_names == {"StepA", "StepB", "StepC"}

    def test_updates_updated_at(self) -> None:
        """mark_step updates the updated_at timestamp to a more recent value."""
        m = _manifest()
        updated = mark_step(m, "StepA", RunStatus.SUCCESS)
        assert updated.updated_at >= m.updated_at


class TestManifestStoreProtocol:
    # ---------------------------------------------------------------------------
    # ManifestStore protocol shape test (structural check only)
    # ---------------------------------------------------------------------------

    def test_is_satisfied_by_a_compatible_class(self) -> None:
        """A class with load/save/delete satisfies the ManifestStore protocol."""

        # Verify ManifestStore has the expected methods via duck-typing check
        class _FakeStore:
            def load(self, correlation_id: str) -> RunManifest | None:
                return None

            def save(self, manifest: RunManifest) -> None:
                """No-op stub: only the method signature is needed for the protocol check."""

            def delete(self, correlation_id: str) -> None:
                """No-op stub: only the method signature is needed for the protocol check."""

        store = _FakeStore()
        # ManifestStore is a Protocol — we verify it has the right attribute names
        assert hasattr(ManifestStore, "load")
        assert hasattr(ManifestStore, "save")
        assert hasattr(ManifestStore, "delete")
        # And our fake satisfies duck typing
        assert hasattr(store, "load")
        assert hasattr(store, "save")
        assert hasattr(store, "delete")
