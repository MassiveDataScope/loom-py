"""FastAPI-specific REST layer for Loom.

Provides the FastAPI binding for compiled REST interfaces:
- :class:`~loom.rest.fastapi.response.MsgspecJSONResponse` — zero-copy JSON response.
- :func:`~loom.rest.fastapi.router.bind_interfaces` — binds compiled routes to FastAPI.
- :class:`~loom.rest.fastapi.runtime.FastAPIAdapter` — the HTTP adapter of ``create_app``.
- :func:`~loom.rest.fastapi.app.create_fastapi_app` — composition root.
- :func:`~loom.rest.fastapi.auto.describe_fastapi_app` — application self-description.
"""

from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.fastapi.auto import create_app, describe_fastapi_app
from loom.rest.fastapi.response import MsgspecJSONResponse
from loom.rest.fastapi.router import bind_interfaces
from loom.rest.fastapi.runtime import FastAPIAdapter

__all__ = [
    "FastAPIAdapter",
    "MsgspecJSONResponse",
    "bind_interfaces",
    "create_app",
    "create_fastapi_app",
    "describe_fastapi_app",
]
