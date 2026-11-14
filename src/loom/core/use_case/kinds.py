"""The use-case execution kinds: ``Caller()``, and the markers no constructor takes."""

from __future__ import annotations

from loom.core.execution import ExecutionKinds, Kind, execution_identity
from loom.core.identity import Identity
from loom.core.use_case.markers import (
    _CallerMarker,
    _ExistsMarker,
    _InputMarker,
    _LoadByIdMarker,
    _LoadMarker,
    _QueryMarker,
)

CALLER = Kind("caller", singleton_safe=False, spelling=_CallerMarker.spelling)

_INPUT_MARKERS = (_InputMarker, _QueryMarker, _LoadByIdMarker, _LoadMarker, _ExistsMarker)


def declare_use_case_kinds(kinds: ExecutionKinds) -> None:
    """Declare ``Caller()`` as :data:`CALLER` and refuse the input markers on ``kinds``.

    The input markers describe the request one ``execute`` call serves, so a
    constructor declaring one is refused.
    """
    kinds.declare_type(_CallerMarker, CALLER)
    for marker in _INPUT_MARKERS:
        kinds.refuse(marker, f"{marker.spelling} declares an input of execute(), not a dependency")


class CallerResolver:
    """Resolves ``Caller()`` to the identity of the execution in progress."""

    __slots__ = ()

    def resolve(self, declaration: object, annotation: object) -> Identity:
        """Return :func:`~loom.core.execution.execution_identity`, failing closed."""
        return execution_identity()
