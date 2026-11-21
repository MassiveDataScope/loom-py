"""The edges of a Lambda function: what reaches the runtime when Init or an invocation raises.

The Python runtime writes an uncaught exception itself, ``errorMessage``,
``errorType`` and ``stackTrace``, and returns it in the invoke response,
without going through ``logging``; Init errors behave the same way. Each edge
hands the runtime a :class:`~loom.core.logger.failure.FailureReference`
instead: the failure is logged once through loom, and the runtime reports its
type, code and trace id. The reference's class is named after the original
type, so ``errorType`` still names it for Step Functions.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import lru_cache
from typing import TYPE_CHECKING, Any, TypeVar

from loom.core.logger.failure import FailureReference, Handoff, reference
from loom.core.logger.install import install_entry_point

if TYPE_CHECKING:
    from loom.serverless.aws.lambda_.handler import LambdaHandler

_T = TypeVar("_T")

_NAMED_REFERENCES = 128
"""Reference classes kept, one per original type name; the oldest is dropped past it."""


class LambdaHandoff(Handoff):
    """Where a Lambda function hands a failure to the runtime."""

    INIT = "lambda.init"
    INVOKE = "lambda.invoke"


@lru_cache(maxsize=_NAMED_REFERENCES)
def _reference_named(exception_type: str) -> type[FailureReference]:
    name = exception_type.rpartition(".")[2]
    return type(name, (FailureReference,), {"__module__": __name__, "__qualname__": name})


def lambda_reference(failure: BaseException) -> FailureReference:
    """The reference the runtime gets for *failure*, its class named after the original type."""
    handed_off = reference(failure)
    return _reference_named(handed_off.exception_type)(*handed_off.args)


def lambda_init(build: Callable[[], _T]) -> _T:
    """Run a function's Init: read its configuration, compose and mount, under a reference.

    The record factory is installed first, before any configuration is read, so
    even a failure to read it is logged safely; the uncaught-exception hooks
    are installed too, as Lambda Init is a runtime entry point.

    Raises:
        FailureReference: In place of any exception *build* raised.
    """
    install_entry_point()
    try:
        return build()
    except Exception as exc:
        failure = lambda_reference(exc)
    raise failure


class LambdaBoundary:
    """A Lambda handler whose failures reach the runtime as a reference.

    Args:
        handler: The handler of the function's trigger.
    """

    def __init__(self, handler: LambdaHandler) -> None:
        self._handler = handler

    def __call__(self, event: dict[str, Any], context: Any) -> Any:
        """Answer one invocation.

        Raises:
            FailureReference: In place of any exception the handler raised.
        """
        try:
            return self._handler(event, context)
        except Exception as exc:
            failure = lambda_reference(exc)
        raise failure

    def close(self) -> None:
        """Close the wrapped handler."""
        self._handler.close()
