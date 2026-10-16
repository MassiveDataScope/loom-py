"""Fingerprint of the facts of a compiled plan that decide its answers."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

import msgspec

from loom.ai.abc import OutputCheck

if TYPE_CHECKING:
    from loom.ai.compiler._plan import AgentPlan, CompiledCapability

_CAPABILITY_NAMES: Final[Mapping[str, tuple[str, ...]]] = MappingProxyType(
    {
        "usecase": ("keys",),
        "sql": ("connection",),
        "mcp": ("server", "include", "exclude"),
        "skills": ("library", "names"),
        "python": ("factory_ref",),
        "a2a": ("agent", "include", "exclude"),
        "native": ("tool",),
    }
)


def plan_fingerprint(plan: AgentPlan) -> str:
    """Return the sha256 of the declared facts that decide *plan*'s answers.

    Args:
        plan: Compiled plan.

    Returns:
        Hex digest over the format version, the compiled instructions (text
        and template), the output schema, the output check's import path,
        every capability's kind and declared facts (``usecase`` keys, ``sql``
        connection, ``mcp`` server, ``skills`` library and names, ``python``
        factory reference, ``a2a`` agent, ``native`` tool, and the
        ``include``/``exclude`` filters of ``mcp`` and ``a2a``) and the bound
        provider, model, output mode and options. No policy enters it.
    """
    facts = {
        "spec_version": plan.spec_version,
        "instructions": [[block.text, block.template] for block in plan.instructions],
        "output_schema": plan.output.schema,
        "output_check": _reference(plan.output_check),
        "capabilities": [_capability(capability) for capability in plan.capabilities],
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


def _capability(capability: CompiledCapability) -> list[object]:
    return [capability.kind, *(getattr(capability, a) for a in _CAPABILITY_NAMES[capability.kind])]
