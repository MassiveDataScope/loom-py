"""Transport-neutral composition: the application, its resources and their lifecycle.

Nothing here imports an HTTP framework, the Celery worker or :mod:`loom.rest`.
"""

from loom.composition.application import Application
from loom.composition.resources import Resource, ResourceKind, closing

__all__ = ["Application", "Resource", "ResourceKind", "closing"]
