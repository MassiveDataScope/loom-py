"""Tests for loom.prefect._ctx.FlowCtx.

Verifies:
- FlowCtx is a frozen msgspec.Struct (immutable).
- Required fields (correlation_id, run_id) raise TypeError when missing.
- Default values for optional fields (dry_run, force, processes).
- No environment of its own: the deployment environment is loom.core.environment's.
- processes=None default and explicit tuple values.
"""

from __future__ import annotations

import pytest

from loom.prefect._ctx import FlowCtx


def test_flow_ctx_required_fields_missing_raises() -> None:
    """FlowCtx must require correlation_id and run_id."""
    with pytest.raises(TypeError):
        FlowCtx()  # type: ignore[call-arg]


def test_flow_ctx_missing_run_id_raises() -> None:
    """FlowCtx must require run_id."""
    with pytest.raises(TypeError):
        FlowCtx(correlation_id="corr-1")  # type: ignore[call-arg]


def test_flow_ctx_missing_correlation_id_raises() -> None:
    """FlowCtx must require correlation_id."""
    with pytest.raises(TypeError):
        FlowCtx(run_id="run-1")  # type: ignore[call-arg]


def test_flow_ctx_minimal_construction() -> None:
    """FlowCtx can be built with only required fields."""
    ctx = FlowCtx(correlation_id="corr-1", run_id="run-1")
    assert ctx.correlation_id == "corr-1"
    assert ctx.run_id == "run-1"


def test_flow_ctx_carries_no_environment_of_its_own() -> None:
    """The environment is resolved once by loom.core.environment, not per flow context."""
    assert "environment" not in FlowCtx.__struct_fields__


def test_flow_ctx_default_dry_run() -> None:
    """dry_run defaults to False."""
    ctx = FlowCtx(correlation_id="corr-1", run_id="run-1")
    assert ctx.dry_run is False


def test_flow_ctx_default_force() -> None:
    """force defaults to False."""
    ctx = FlowCtx(correlation_id="corr-1", run_id="run-1")
    assert ctx.force is False


def test_flow_ctx_default_processes_is_none() -> None:
    """processes defaults to None."""
    ctx = FlowCtx(correlation_id="corr-1", run_id="run-1")
    assert ctx.processes is None


def test_flow_ctx_explicit_processes_tuple() -> None:
    """processes can be set as a tuple of strings."""
    ctx = FlowCtx(
        correlation_id="corr-1",
        run_id="run-1",
        processes=("StepA", "StepB"),
    )
    assert ctx.processes == ("StepA", "StepB")


def test_flow_ctx_is_immutable() -> None:
    """FlowCtx must be frozen (immutable)."""
    ctx = FlowCtx(correlation_id="corr-1", run_id="run-1")
    with pytest.raises((TypeError, AttributeError)):
        ctx.correlation_id = "other"  # type: ignore[misc]


def test_flow_ctx_full_construction() -> None:
    """FlowCtx accepts all fields."""
    ctx = FlowCtx(
        correlation_id="corr-42",
        run_id="run-42",
        dry_run=True,
        force=True,
        processes=("ProcA",),
    )
    assert ctx.dry_run is True
    assert ctx.force is True
    assert ctx.processes == ("ProcA",)
