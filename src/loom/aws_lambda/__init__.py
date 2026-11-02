"""AWS Lambda transport adapters: API Gateway HTTP events and SQS batches.

The names load on first access, so importing the SQS adapter never loads the
HTTP stack (FastAPI, Starlette, Mangum). The HTTP adapter needs the ``lambda``
extra; the SQS adapter needs none::

    pip install "loom-kernel[lambda]"
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from loom.aws_lambda._refusals import LambdaUnservedConfigError
    from loom.aws_lambda.http import LambdaHTTPAdapter, LambdaHTTPHandler
    from loom.aws_lambda.sqs import LambdaSQSAdapter

_EXPORTS = {
    "LambdaHTTPAdapter": "loom.aws_lambda.http",
    "LambdaHTTPHandler": "loom.aws_lambda.http",
    "LambdaSQSAdapter": "loom.aws_lambda.sqs",
    "LambdaUnservedConfigError": "loom.aws_lambda._refusals",
}

__all__ = [
    "LambdaHTTPAdapter",
    "LambdaHTTPHandler",
    "LambdaSQSAdapter",
    "LambdaUnservedConfigError",
]


def __getattr__(name: str) -> Any:
    """Import a public name from its module on first access (PEP 562)."""
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
