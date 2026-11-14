"""Limits phase failures (T050): every ``PolicySpec`` range, below and above."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any, cast

import pytest

import loom.ai.compiler  # noqa: F401  — red until the compiler exists
from loom.ai.declarative import AgentSpecV1, PolicySpec
from loom.ai.declarative._v1 import (
    MAX_HISTORY_BYTES_MAX,
    MAX_HISTORY_BYTES_MIN,
    MAX_INPUT_TOKENS_PER_REQUEST_MAX,
    MAX_INPUT_TOKENS_PER_REQUEST_MIN,
    MAX_ITERATIONS_MAX,
    MAX_ITERATIONS_MIN,
    MAX_REQUESTS_MAX,
    MAX_REQUESTS_MIN,
    MAX_TOOL_CALLS_MAX,
    MAX_TOOL_CALLS_MIN,
    MAX_TOTAL_TOKENS_MAX,
    MAX_TOTAL_TOKENS_MIN,
    RETRIES_MAX,
    RETRIES_MIN,
    RUN_TIMEOUT_MS_MAX,
    RUN_TIMEOUT_MS_MIN,
    TOOL_TIMEOUT_MS_MAX,
    TOOL_TIMEOUT_MS_MIN,
)
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode

_RANGES: dict[str, tuple[int, int]] = {
    "retries": (RETRIES_MIN, RETRIES_MAX),
    "tool_timeout_ms": (TOOL_TIMEOUT_MS_MIN, TOOL_TIMEOUT_MS_MAX),
    "max_iterations": (MAX_ITERATIONS_MIN, MAX_ITERATIONS_MAX),
    "run_timeout_ms": (RUN_TIMEOUT_MS_MIN, RUN_TIMEOUT_MS_MAX),
    "max_history_bytes": (MAX_HISTORY_BYTES_MIN, MAX_HISTORY_BYTES_MAX),
}

# The four new *optional* integer caps (FR-040): unlike ``_RANGES`` above,
# ``None`` is their default and must compile clean rather than sit on a bound.
_OPTIONAL_RANGES: dict[str, tuple[int, int]] = {
    "max_total_tokens": (MAX_TOTAL_TOKENS_MIN, MAX_TOTAL_TOKENS_MAX),
    "max_input_tokens_per_request": (
        MAX_INPUT_TOKENS_PER_REQUEST_MIN,
        MAX_INPUT_TOKENS_PER_REQUEST_MAX,
    ),
    "max_tool_calls": (MAX_TOOL_CALLS_MIN, MAX_TOOL_CALLS_MAX),
    "max_requests": (MAX_REQUESTS_MIN, MAX_REQUESTS_MAX),
}


def _out_of_range_cases() -> list[tuple[str, int]]:
    cases: list[tuple[str, int]] = []
    for name, (minimum, maximum) in _RANGES.items():
        cases.append((name, minimum - 1))
        cases.append((name, maximum + 1))
    return cases


class TestPolicyRanges:
    @pytest.mark.parametrize(
        ("policy", "value"),
        _out_of_range_cases(),
        ids=[f"{name}_{value}" for name, value in _out_of_range_cases()],
    )
    def test_reports_policy_out_of_range_when_value_leaves_its_range(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        policy: str,
        value: int,
    ) -> None:
        spec = spec_factory(policies=PolicySpec(**cast("dict[str, Any]", {policy: value})))
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.POLICY_OUT_OF_RANGE,
            f"policies.{policy}",
        )

    @pytest.mark.parametrize("bound", [0, 1], ids=["minima", "maxima"])
    def test_compiles_clean_when_every_policy_sits_on_its_inclusive_bound(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        compiler_factory: Callable[..., object],
        bound: int,
    ) -> None:
        values = {name: limits[bound] for name, limits in _RANGES.items()}
        spec = spec_factory(policies=PolicySpec(**cast("dict[str, Any]", values)))
        plan = compiler_factory().compile(spec)  # type: ignore[attr-defined]
        assert plan.policies == PolicySpec(**cast("dict[str, Any]", values))


def _optional_out_of_range_cases() -> list[tuple[str, int]]:
    cases: list[tuple[str, int]] = []
    for name, (minimum, maximum) in _OPTIONAL_RANGES.items():
        cases.append((name, minimum - 1))
        cases.append((name, maximum + 1))
    return cases


class TestOptionalSpendCaps:
    @pytest.mark.parametrize(
        ("policy", "value"),
        _optional_out_of_range_cases(),
        ids=[f"{name}_{value}" for name, value in _optional_out_of_range_cases()],
    )
    def test_reports_policy_out_of_range_for_an_optional_spend_cap(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        policy: str,
        value: int,
    ) -> None:
        """The four optional integer caps of FR-040 are checked exactly like the five older ones."""
        spec = spec_factory(policies=PolicySpec(**cast("dict[str, Any]", {policy: value})))
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.POLICY_OUT_OF_RANGE,
            f"policies.{policy}",
        )

    def test_compiles_clean_when_every_optional_spend_cap_is_absent(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        compiler_factory: Callable[..., object],
    ) -> None:
        """``None`` is never out of range (FR-042): today's behaviour with no ``policies``."""
        spec = spec_factory()
        plan = compiler_factory().compile(spec)  # type: ignore[attr-defined]
        assert plan.policies.max_total_tokens is None
        assert plan.policies.max_input_tokens_per_request is None
        assert plan.policies.max_tool_calls is None
        assert plan.policies.max_requests == 50


class TestMaxUsdBounds:
    @pytest.mark.parametrize("delta", [Decimal("-0.01"), Decimal("100000.01")])
    def test_reports_policy_out_of_range_for_max_usd(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        single_issue_for: Callable[..., AgentCompilationIssue],
        delta: Decimal,
    ) -> None:
        """``max_usd`` is a ``Decimal``, checked outside the int-typed table."""
        bound = Decimal("0.01") if delta < 0 else Decimal("100000")
        spec = spec_factory(policies=PolicySpec(max_usd=bound + delta))
        issue = single_issue_for(spec)
        assert (issue.code, issue.field) == (
            AgentErrorCode.POLICY_OUT_OF_RANGE,
            "policies.max_usd",
        )

    def test_compiles_clean_when_max_usd_sits_on_its_inclusive_bound(
        self,
        spec_factory: Callable[..., AgentSpecV1],
        compiler_factory: Callable[..., object],
    ) -> None:
        spec = spec_factory(policies=PolicySpec(max_usd=Decimal("100000")))
        plan = compiler_factory().compile(spec)  # type: ignore[attr-defined]
        assert plan.policies.max_usd == Decimal("100000")
