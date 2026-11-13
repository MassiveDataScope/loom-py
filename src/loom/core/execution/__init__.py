"""The execution in progress: its identity, read at call time, and its cache.

Every use case, job or agent run happens inside an execution. This package
publishes that execution so code can ask who it runs as
(:func:`execution_identity`) and share what it built once per execution
(:class:`ExecutionContext`), and it keeps which declarations resolve per
execution (:class:`ExecutionKinds`). It imports only :mod:`loom.core.identity` and
:mod:`loom.core.errors`; the container, the engine, the use cases and the AI
pillar build on it, never the reverse.
"""

from loom.core.execution.context import (
    ExecutionContext,
    ExecutionScope,
    NoExecution,
    current_execution,
    enter,
    execution_identity,
    leave,
    per_execution,
)
from loom.core.execution.errors import ExecutionRequired, MarkerNotResolved
from loom.core.execution.resolvers import ExecutionKinds, ExecutionResolver, Kind

__all__ = [
    "ExecutionContext",
    "ExecutionKinds",
    "ExecutionResolver",
    "ExecutionRequired",
    "ExecutionScope",
    "Kind",
    "MarkerNotResolved",
    "NoExecution",
    "current_execution",
    "enter",
    "execution_identity",
    "leave",
    "per_execution",
]
