"""Transport-neutral composition: the application, its resources and their lifecycle.

An adapter declares a :class:`Recipe`; :func:`create_app` composes the
:class:`Application` it asks for and lets the adapter mount its transport.
Nothing here imports an HTTP framework, the Celery worker or :mod:`loom.rest`,
but for one sanctioned reach-out: :func:`create_app` without an adapter imports
the one ``app.entry`` names, lazily, when it runs.
"""

from loom.composition._ai import AiWiring
from loom.composition._jobs import JobBackendContext
from loom.composition._sql import SqlWiring
from loom.composition.application import Application
from loom.composition.build import create_app
from loom.composition.errors import (
    ApplicationAlreadyOpenError,
    CompositionErrorCode,
    CompositionStateError,
    JobBackendError,
    ReservedBindingError,
)
from loom.composition.host import ApplicationHost, LoopHost
from loom.composition.project import AppConfig, Project
from loom.composition.recipe import Adapter, Recipe

__all__ = [
    "Adapter",
    "AiWiring",
    "AppConfig",
    "Application",
    "ApplicationAlreadyOpenError",
    "ApplicationHost",
    "CompositionErrorCode",
    "CompositionStateError",
    "JobBackendContext",
    "JobBackendError",
    "LoopHost",
    "Project",
    "Recipe",
    "ReservedBindingError",
    "SqlWiring",
    "create_app",
]
