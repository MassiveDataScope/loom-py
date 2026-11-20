"""Doubles shared by the host unit tests."""

from __future__ import annotations

import msgspec

from loom.ai.compiler._plan import AgentPlan, CompiledInstruction, CompiledOutput
from loom.ai.declarative import PolicySpec
from loom.ai.inference import InferenceTarget
from loom.core.model import msgspec_type


class AcmeDefaultedState(msgspec.Struct, forbid_unknown_fields=True):
    """A state shape whose every field carries a declared default."""

    marca: str = ""
    km: int = 0


class AcmeRequiredState(msgspec.Struct, forbid_unknown_fields=True):
    """A state shape with one field carrying no declared default."""

    marca: str


def incident_triage_plan() -> AgentPlan:
    """One compiled plan shared by the hook and loader command tests."""
    return AgentPlan(
        name="incident-triage",
        description="test agent",
        instructions=(CompiledInstruction(text="answer"),),
        spec_version=1,
        inference=InferenceTarget(provider="fake", model="fake-model"),
        output=CompiledOutput(schema={"type": "object"}, loom_type=msgspec_type(dict)),
        capabilities=(),
        policies=PolicySpec(),
        metadata={},
    )
