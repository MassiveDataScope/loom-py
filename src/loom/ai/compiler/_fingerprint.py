"""Fingerprint of the facts of a compiled plan that decide its answers."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import TYPE_CHECKING

import msgspec

from loom.ai.abc import OutputCheck

if TYPE_CHECKING:
    from loom.ai.compiler._plan import AgentPlan


def plan_fingerprint(plan: AgentPlan) -> str:
    """Return the sha256 of what decides *plan*'s answers.

    Args:
        plan: Compiled plan.

    Returns:
        Hex digest over the instructions, output schema and check, policies,
        format version and model binding, without the deployment details.
    """
    facts = {
        "spec_version": plan.spec_version,
        "instructions": [[block.text, block.template] for block in plan.instructions],
        "output_schema": plan.output.schema,
        "output_check": _reference(plan.output_check),
        "policies": plan.policies,
        "provider": plan.inference.provider,
        "model": plan.inference.model,
        "output_mode": plan.inference.output_mode,
        "options": plan.inference.options,
    }
    encoded = msgspec.json.encode(facts, order="sorted", enc_hook=_as_dict)
    return hashlib.sha256(encoded).hexdigest()


def _as_dict(value: object) -> dict[object, object]:
    if isinstance(value, Mapping):
        return dict(value)
    raise NotImplementedError(f"cannot fingerprint {type(value).__name__}")


def _reference(check: OutputCheck | None) -> str | None:
    if check is None:
        return None
    return f"{check.__module__}:{check.__qualname__}"
