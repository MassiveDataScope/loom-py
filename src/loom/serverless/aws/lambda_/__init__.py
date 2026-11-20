"""AWS Lambda: one adapter serving the trigger each deployed function declares.

The names load on first access, so a function running jobs or events never
loads the HTTP stack (FastAPI, Starlette, Mangum). An ``http`` function needs
the ``lambda`` extra; the others need none::

    pip install "loom-kernel[lambda]"
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from loom.serverless.aws.lambda_._refusals import LambdaUnservedConfigError
    from loom.serverless.aws.lambda_.handler import LambdaHandler, LambdaRuntime
    from loom.serverless.aws.lambda_.triggers.s3 import S3Object

_EXPORTS = {
    "LambdaRuntime": "loom.serverless.aws.lambda_.handler",
    "LambdaHandler": "loom.serverless.aws.lambda_.handler",
    "LambdaUnservedConfigError": "loom.serverless.aws.lambda_._refusals",
    "S3Object": "loom.serverless.aws.lambda_.triggers.s3",
}

__all__ = [
    "LambdaRuntime",
    "LambdaHandler",
    "LambdaUnservedConfigError",
    "S3Object",
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
