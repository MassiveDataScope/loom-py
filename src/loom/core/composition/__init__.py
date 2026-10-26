"""Transport-neutral composition: the application and its lifecycle.

Nothing here imports an HTTP framework, a queue broker or :mod:`loom.rest`.
"""

from loom.core.composition.application import Application

__all__ = ["Application"]
