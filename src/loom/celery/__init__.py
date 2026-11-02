"""Loom Celery integration.

Provides distributed job dispatching via Celery.  All symbols here are
considered part of the public API of the optional ``loom[celery]`` extra.

The names load on first access: the composition imports
:mod:`loom.celery.config` and :mod:`loom.celery.service` to dispatch jobs,
and must not load the worker (its adapter, bootstrap and Celery signals)
by doing so.

Install with::

    pip install "loom-py[celery]"
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from loom.celery.auto import create_app
    from loom.celery.bootstrap import WorkerBootstrapResult, bootstrap_worker
    from loom.celery.service import CeleryJobService

_EXPORTS = {
    "create_app": "loom.celery.auto",
    "bootstrap_worker": "loom.celery.bootstrap",
    "WorkerBootstrapResult": "loom.celery.bootstrap",
    "CeleryJobService": "loom.celery.service",
}

__all__ = [
    "create_app",
    "bootstrap_worker",
    "WorkerBootstrapResult",
    "CeleryJobService",
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
