"""Feedability proof shared by the output-hook and conversation phases.

A use case is feedable from a run when its compiled execution plan declares
no primitive parameters, one ``Input()`` whose type builds from a payload,
and every required, user-supplied Input name lies inside the set of names
the runtime offers.  ``internal`` and ``calculated`` command fields are
infrastructure-owned and never demanded; ``Caller()`` is injected by the
executor and never demanded either.

Use cases are compiled by the bootstrap before the agent compiler runs, so
the proof reads ``__execution_plan__`` and never calls the use-case compiler
itself — an uncompiled use case is a refusal, not a job.
"""

from __future__ import annotations

from collections.abc import Sequence

import msgspec

from loom.ai.declarative import UsecaseCapability
from loom.core.command.introspection import get_command_fields, get_input_fields
from loom.core.engine.compilable import Compilable
from loom.core.engine.plan import ExecutionPlan


def is_granted(capabilities: Sequence[object], key: str) -> bool:
    """Whether a ``kind: usecase`` grant of the spec lists ``key``.

    Checked on the spec rather than on the compiled capability so the conflict
    is reported even when that grant fails on another key.
    """
    return any(
        key in capability.keys
        for capability in capabilities
        if isinstance(capability, UsecaseCapability)
    )


def feedable_input(
    use_case: type[Compilable], offered: frozenset[str]
) -> tuple[frozenset[str], str | None]:
    """Return the Input's declared names, or the reason the Input is unfeedable.

    Args:
        use_case: Compiled use case whose Input the run must feed.
        offered: Names the runtime offers to that Input.

    Returns:
        The declared names and ``None``; an empty set and the reason otherwise.
    """
    execution = use_case.__execution_plan__
    if execution is None:
        return frozenset(), "the use case is not compiled (no __execution_plan__)"
    reason = _binding_reason(execution)
    if reason is not None or execution.input_binding is None:
        return frozenset(), reason
    command_type = execution.input_binding.command_type
    declared = {info.name: info for info in msgspec.structs.fields(command_type)}
    excluded = get_command_fields(command_type).keys() - get_input_fields(command_type).keys()
    extra = [
        name
        for name, info in declared.items()
        if info.required and name not in excluded and name not in offered
    ]
    if extra:
        return frozenset(), (
            f"Input requires {', '.join(sorted(extra))} but the run offers only "
            f"{', '.join(sorted(offered))}"
        )
    return frozenset(declared), None


def _binding_reason(execution: ExecutionPlan) -> str | None:
    """Reason the execution plan's bindings cannot be fed from a run, if any."""
    if execution.param_bindings:
        names = ", ".join(binding.name for binding in execution.param_bindings)
        return f"execute declares primitive parameters the run cannot bind: {names}"
    if execution.input_binding is None:
        return "execute declares no Input()"
    # Defensive: ``UseCaseCompiler`` already refuses an Input type without
    # ``from_payload``, but a plan built through another path must not pass.
    if not callable(getattr(execution.input_binding.command_type, "from_payload", None)):
        return "the Input type does not implement from_payload(payload)"
    return None
