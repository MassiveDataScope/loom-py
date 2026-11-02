"""Transport-neutral composition: the application, its resources and their lifecycle.

An adapter declares a :class:`Recipe`; :func:`create_app` composes the
:class:`Application` it asks for and lets the adapter mount its transport.
Nothing here imports an HTTP framework, the Celery worker or :mod:`loom.rest`.
"""

from loom.composition._ai import AiWiring
from loom.composition._sql import SqlWiring
from loom.composition.application import Application
from loom.composition.build import build_application, compose_application, create_app, load_source
from loom.composition.errors import (
    ApplicationAlreadyOpenError,
    CompositionErrorCode,
    ReservedBindingError,
)
from loom.composition.host import ApplicationHost
from loom.composition.recipe import Adapter, Recipe
from loom.composition.source import AppConfig, CompositionSource

__all__ = [
    "Adapter",
    "AiWiring",
    "AppConfig",
    "Application",
    "ApplicationAlreadyOpenError",
    "ApplicationHost",
    "CompositionErrorCode",
    "CompositionSource",
    "Recipe",
    "ReservedBindingError",
    "SqlWiring",
    "build_application",
    "compose_application",
    "create_app",
    "load_source",
]
