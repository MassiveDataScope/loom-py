"""The edges of a Lambda function: what reaches the runtime when Init or an invocation raises.

The Python runtime writes an uncaught exception itself, ``errorMessage``,
``errorType`` and ``stackTrace``, and returns it in the invoke response,
without going through ``logging``; Init errors behave the same way. Both edges
go through the loom boundary (:func:`~loom.core.errors.boundary.boundary`):
the failure is logged once through loom, and the runtime reports a clean
``LoomError``, a foreign failure as
:class:`~loom.core.errors.UnexpectedError`, whose message names the original
type only.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

from loom.core.errors.boundary import boundary
from loom.core.logger.install import install_entry_point

if TYPE_CHECKING:
    from loom.serverless.aws.lambda_.handler import LambdaHandler

_T = TypeVar("_T")


def lambda_init(build: Callable[[], _T]) -> _T:
    """Run a function's Init: read its configuration, compose and mount, through the boundary.

    The record factory goes in first, before any configuration is read, so
    even a failure to read it is logged safely; the process hooks go in too,
    for the life of the execution environment, as Init is a runtime entry point.
    """
    install_entry_point()
    return boundary(build)()


class LambdaBoundary:
    """A Lambda handler whose invocations go through the loom boundary.

    Args:
        handler: The handler of the function's trigger.
    """

    def __init__(self, handler: LambdaHandler) -> None:
        self._handler = handler
        self._invoke = boundary(handler)

    def __call__(self, event: dict[str, Any], context: Any) -> Any:
        """Answer one invocation; a failure leaves as a clean ``LoomError``."""
        return self._invoke(event, context)

    def close(self) -> None:
        """Close the wrapped handler."""
        self._handler.close()
