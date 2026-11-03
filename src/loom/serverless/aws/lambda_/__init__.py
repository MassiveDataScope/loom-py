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
    from loom.serverless.aws.lambda_._refusals import LambdaUnservedConfigError
    from loom.serverless.aws.lambda_.http import LambdaHTTPAdapter, LambdaHTTPHandler
    from loom.serverless.aws.lambda_.sqs import LambdaSQSAdapter

_EXPORTS = {
    "LambdaHTTPAdapter": "loom.serverless.aws.lambda_.http",
    "LambdaHTTPHandler": "loom.serverless.aws.lambda_.http",
    "LambdaSQSAdapter": "loom.serverless.aws.lambda_.sqs",
    "LambdaUnservedConfigError": "loom.serverless.aws.lambda_._refusals",
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
