"""The behavioural canary: a value put where values hide, which no output loom emits may carry."""

from __future__ import annotations

CANARY = "canary-7f3a9c"


class Leaky:
    """An object whose text is the canary, as a driver's or a model's often is."""

    def __repr__(self) -> str:
        return CANARY

    __str__ = __repr__


def canary_failure() -> RuntimeError:
    """A failure with the canary in its message, its cause, its note and a group member."""
    try:
        try:
            raise ExceptionGroup(CANARY, [KeyError(CANARY)])
        except ExceptionGroup as cause:
            raise RuntimeError(CANARY) from cause
    except RuntimeError as error:
        error.add_note(CANARY)
        return error


def raise_canary_failure(*_: object, **__: object) -> None:
    """Raise :func:`canary_failure`: a stand-in for any work that fails."""
    raise canary_failure()
