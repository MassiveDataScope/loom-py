"""The execution in progress: its identity, read at call time, and its cache.

Every use case, job or agent run happens inside an execution. This package
publishes that execution so code can ask who it runs as
(:func:`execution_identity`) and share what it built once per execution
(:class:`ExecutionContext`). It imports only :mod:`loom.core.identity` and
:mod:`loom.core.errors`; the container, the engine, the use cases and the AI
pillar build on it, never the reverse.
"""

from loom.core.execution.context import (
    ExecutionContext,
    ExecutionScope,
    NoExecution,
    current_execution,
    detached,
    enter,
    execution_identity,
    leave,
)
from loom.core.execution.errors import ExecutionRequired, MarkerNotResolved

__all__ = [
    "ExecutionContext",
    "ExecutionRequired",
    "ExecutionScope",
    "MarkerNotResolved",
    "NoExecution",
    "current_execution",
    "detached",
    "enter",
    "execution_identity",
    "leave",
]
