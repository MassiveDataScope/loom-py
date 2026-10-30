"""Transport-neutral composition: the application, its resources and their lifecycle.

An adapter declares a :class:`Recipe`; :func:`create_app` composes the
:class:`Application` it asks for and lets the adapter mount its transport.
Nothing here imports an HTTP framework, the Celery worker or :mod:`loom.rest`.
"""

from loom.composition.application import Application
from loom.composition.build import build_application, compose_application, create_app
from loom.composition.errors import CompositionErrorCode, ReservedBindingError
from loom.composition.host import ApplicationHost
from loom.composition.recipe import Adapter, Recipe
from loom.composition.resources import Resource, ResourceKind, closing
from loom.composition.source import AppConfig, CompositionSource

__all__ = [
    "Adapter",
    "AppConfig",
    "Application",
    "ApplicationHost",
    "CompositionErrorCode",
    "CompositionSource",
    "Recipe",
    "ReservedBindingError",
    "Resource",
    "ResourceKind",
    "build_application",
    "closing",
    "compose_application",
    "create_app",
]
