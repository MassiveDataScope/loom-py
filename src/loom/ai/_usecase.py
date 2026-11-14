"""Engine-free helper to run a granted use case as a given caller.

The pydantic-ai capability tools, the runtime output hook and the conversation
loader invoke a use case through the dependency bundle's **bound** invoker
(``invoker.for_identity(caller)``): the executor acts as that caller, and logs
the switch when it is not the identity already acting. Nothing installs an
ambient identity here. This module holds the shared lookup without importing
any engine, so the runtime can use it with no optional extra installed.
"""

from __future__ import annotations

from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.use_case.invoker import ApplicationInvoker


def require_invoker(bundle: object, label: str) -> ApplicationInvoker:
    """Return the bundle's invoker, refusing a bundle that carries none.

    Read from the bundle rather than resolved from the container: the bundle's
    invoker is the one the composition root already bound to this invocation's
    caller, and the container holds only the unbound singleton.

    Args:
        bundle: Dependency bundle built for the caller by the deps factory.
        label: Caller of the refused call, worded as it should appear in the
            error (``"tool 'lookup'"``, ``"on_output hook 'incidents.record'"``).

    Returns:
        The invoker bound to the caller.

    Raises:
        AgentRunError: ``UNAUTHORIZED`` when the bundle carries no invoker.
    """
    invoker = getattr(bundle, "invoker", None)
    if isinstance(invoker, ApplicationInvoker):
        return invoker
    raise AgentRunError(
        AgentRunErrorCode.UNAUTHORIZED,
        f"{label} requires a dependency bundle exposing an 'invoker' bound to its caller",
    )
