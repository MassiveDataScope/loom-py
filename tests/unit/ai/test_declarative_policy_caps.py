"""Authored spend caps of ``PolicySpec`` (spec 016, T103).

``policies.max_usd``, ``max_total_tokens``, ``max_input_tokens_per_request``,
``max_tool_calls`` and ``max_requests`` are the artifact half of the engine's
``UsageLimits``. These tests pin the decoded shape only: absent caps default
to the engine's own ``request_limit`` of 50 and ``None`` everywhere else
(FR-041, FR-042), and a JSON artifact's ``max_usd`` survives the round trip
as an exact ``Decimal`` rather than through binary floating point (FR-043).
A YAML artifact does not share that guarantee: ``msgspec``'s YAML decoder
parses the same literal through a binary double first, so the two formats
can disagree on the same written value — pinned below alongside the JSON
case. Range enforcement is a compilation concern and lives in
``tests/unit/ai/phases/test_limits.py``.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from msgspec import yaml as msgspec_yaml

from loom.ai.declarative import AgentSpecV1, decode_spec
from loom.ai.declarative._v1 import MAX_REQUESTS_DEFAULT, PolicySpec


def _payload(**policy_overrides: Any) -> dict[str, Any]:
    """Build a minimal v1 artifact, optionally declaring a ``policies`` block."""
    payload: dict[str, Any] = {
        "spec_version": 1,
        "name": "incident-triage",
        "description": "Classifies an incident from its description.",
        "instructions": "Read the incident description and return a report.",
        "output": {"kind": "json_schema", "schema": {"type": "object"}},
    }
    if policy_overrides:
        payload["policies"] = policy_overrides
    return payload


def _decode(payload: dict[str, Any]) -> AgentSpecV1:
    spec = decode_spec(json.dumps(payload).encode("utf-8")).spec
    assert isinstance(spec, AgentSpecV1)
    return spec


class TestPolicyCapDecoding:
    def test_an_absent_policies_block_decodes_to_todays_defaults(self) -> None:
        """No ``policies`` block still means ``request_limit == 50`` and no other cap (AC-010)."""
        spec = _decode(_payload())

        assert spec.policies.max_requests == MAX_REQUESTS_DEFAULT == 50
        assert spec.policies.max_usd is None
        assert spec.policies.max_total_tokens is None
        assert spec.policies.max_input_tokens_per_request is None
        assert spec.policies.max_tool_calls is None
        assert spec.policies.on_unpriced_spend == "serve"

    def test_on_unpriced_spend_declared_without_max_usd_changes_nothing(self) -> None:
        """``on_unpriced_spend`` governs an existing cap; without ``max_usd`` it is inert."""
        spec = _decode(_payload(on_unpriced_spend="refuse"))

        assert spec.policies.on_unpriced_spend == "refuse"
        assert spec.policies.max_usd is None

    def test_on_unpriced_spend_refuse_survives_the_round_trip_alongside_max_usd(self) -> None:
        """The declared policy and the cap it governs decode together, unchanged."""
        spec = _decode(_payload(max_usd=2, on_unpriced_spend="refuse"))

        assert spec.policies == PolicySpec(max_usd=Decimal(2), on_unpriced_spend="refuse")

    def test_max_usd_survives_the_round_trip_as_an_exact_decimal(self) -> None:
        """``19.99`` decodes to an exact ``Decimal``, never through binary float (FR-043).

        ``19.99`` is deliberately not exactly representable in binary floating
        point: ``Decimal(19.99) != Decimal("19.99")``, so a regression that
        converts through ``float`` on its way here — rather than through the
        value's own string form — fails this assertion instead of passing it by
        coincidence, as ``2.00`` would.
        """
        spec = _decode(_payload(max_usd=19.99))

        assert spec.policies.max_usd == Decimal("19.99")
        assert isinstance(spec.policies.max_usd, Decimal)

    def test_max_usd_through_yaml_loses_precision_a_json_artifact_would_keep(self) -> None:
        """The divergent half of FR-043: a literal with more than ~15 significant
        digits round-trips exactly through JSON but not through YAML.

        ``0.1234567890123456789`` is written as a raw numeric token in both
        bodies below, never through a Python ``float`` literal, so neither
        decoder's outcome depends on precision already lost before either of
        them ever saw the value.
        """
        literal = "0.1234567890123456789"
        json_body = (
            b'{"spec_version": 1, "name": "incident-triage", '
            b'"description": "Classifies an incident from its description.", '
            b'"instructions": "Read the incident description and return a report.", '
            b'"output": {"kind": "json_schema", "schema": {"type": "object"}}, '
            b'"policies": {"max_usd": ' + literal.encode("utf-8") + b"}}"
        )
        yaml_body = f"""
spec_version: 1
name: incident-triage
description: Classifies an incident from its description.
instructions: Read the incident description and return a report.
output:
  kind: json_schema
  schema:
    type: object
policies:
  max_usd: {literal}
""".encode()

        json_spec = decode_spec(json_body).spec
        yaml_spec = msgspec_yaml.decode(yaml_body, type=AgentSpecV1)

        assert json_spec.policies.max_usd == Decimal(literal)
        assert yaml_spec.policies.max_usd != Decimal(literal)

    def test_the_five_declared_caps_decode_to_their_values(self) -> None:
        """Every declared cap reaches the struct unchanged."""
        spec = _decode(
            _payload(
                max_usd=5,
                max_total_tokens=10_000,
                max_input_tokens_per_request=2_000,
                max_tool_calls=8,
                max_requests=30,
            )
        )

        assert spec.policies == PolicySpec(
            max_usd=Decimal(5),
            max_total_tokens=10_000,
            max_input_tokens_per_request=2_000,
            max_tool_calls=8,
            max_requests=30,
        )
