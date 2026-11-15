"""Reading the identity an execution acts as, at call time."""

from __future__ import annotations

from loom.core.di.container import LoomContainer
from loom.core.execution import ExecutionScope, execution_identity
from loom.core.identity import Delegation, Identity, captured_delegation


class AuditStamp:
    """A singleton that serves every caller: it reads the identity on each call."""

    def stamp(self, action: str) -> str:
        return f"{execution_identity().subject}:{action}"


def stamp_as(stamp: AuditStamp, caller: Identity) -> str:
    """Run ``stamp`` in an execution acting as *caller*."""
    with ExecutionScope(caller, what="stamp_as"):
        return stamp.stamp("approved")


class Basket:
    """A per-execution binding (``Scope.REQUEST``)."""


def baskets(container: LoomContainer, caller: Identity) -> tuple[Basket, Basket, Basket]:
    """Resolve ``Basket`` twice in one execution and once in a nested one."""
    with ExecutionScope(caller, what="outer"):
        first = container.resolve(Basket)
        again = container.resolve(Basket)
        with ExecutionScope(caller, what="nested"):
            nested = container.resolve(Basket)
    return first, again, nested


def captured_by_dispatch(caller: Identity) -> Delegation:
    """What a job dispatched inside an execution acting as *caller* carries."""
    with ExecutionScope(caller, what="dispatch"):
        return captured_delegation()
