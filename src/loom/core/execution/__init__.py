"""The execution in progress: its identity, read at call time, and its cache.

Every use case, job or agent run happens inside an execution. This package
publishes that execution so code can ask who it acts as
(:func:`execution_identity`, the acting identity it fixed) and share what it
built once per execution (:class:`ExecutionContext`), and it keeps which declarations resolve per
execution (:class:`ExecutionKinds`). It imports only :mod:`loom.core.identity`,
:mod:`loom.core.errors` and :mod:`loom.core.logger`; the container, the
engine, the use cases and the AI pillar build on it, never the reverse.
"""

from loom.core.execution.context import (
    ExecutionContext,
    ExecutionScope,
    NoExecution,
    current_execution,
    enter,
    execution_ended,
    execution_identity,
    leave,
    per_execution,
)
from loom.core.execution.errors import ExecutionClosed, ExecutionRequired, MarkerNotResolved
from loom.core.execution.kinds import (
    ExecutionBinding,
    ExecutionKinds,
    ExecutionResolver,
    Kind,
    UnboundKind,
)

__all__ = [
    "ExecutionBinding",
    "ExecutionClosed",
    "ExecutionContext",
    "ExecutionKinds",
    "ExecutionResolver",
    "ExecutionRequired",
    "ExecutionScope",
    "Kind",
    "MarkerNotResolved",
    "NoExecution",
    "UnboundKind",
    "current_execution",
    "enter",
    "execution_ended",
    "execution_identity",
    "leave",
    "per_execution",
]
