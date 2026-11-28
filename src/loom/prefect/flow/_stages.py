"""Process/step name validation shared by the flow factories and bodies."""

from __future__ import annotations

from typing import Any

from loom.core.errors import InvalidType, InvalidValue
from loom.etl.compiler._plan import PipelinePlan, iter_all_steps, iter_processes


def known_process_names(plan: PipelinePlan) -> frozenset[str]:
    """Process class names in *plan* — the vocabulary of the ``processes`` kwarg."""
    return frozenset(proc.process_type.__name__ for proc in iter_processes(plan))


def known_stage_names(plan: PipelinePlan) -> frozenset[str]:
    """Process AND step class names — what ``ETLRunner.run(include=...)`` accepts."""
    names = {proc.process_type.__name__ for proc in iter_processes(plan)}
    names.update(step.step_type.__name__ for step in iter_all_steps(plan))
    return frozenset(names)


def validate_stage_names(
    raw: Any,
    known: frozenset[str],
    *,
    field: str = "processes",
) -> tuple[str, ...] | None:
    """Validate a user-supplied list of process/step names against *known*.

    Args:
        raw: The untrusted value (flow kwarg or factory argument).
        known: Accepted names, from :func:`known_process_names` or
            :func:`known_stage_names`.
        field: Parameter name used in error messages.

    Returns:
        The names as a tuple, or ``None`` when *raw* is ``None`` or empty.

    Raises:
        TypeError: When *raw* is not ``list[str] | None``.
        ValueError: When any name is not in *known*.
    """
    if raw is None:
        return None
    if not isinstance(raw, (list, tuple)) or not all(isinstance(v, str) for v in raw):
        raise InvalidType(f"{field}: expected list[str] | None")
    requested = tuple(raw)
    if not requested:
        return None
    unknown = [name for name in requested if name not in known]
    if unknown:
        raise InvalidValue(f"{field}: unknown names; known names are {sorted(known)}")
    return requested


__all__ = ["known_process_names", "known_stage_names", "validate_stage_names"]
