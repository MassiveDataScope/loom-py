"""Internal dependency injection container for Loom.

Provides a minimal, framework-agnostic container with application and
request scopes.  No third-party DI library is required.
"""

from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.errors import ContainerSealed
from loom.core.di.installer import ConfiguredInstaller, Installer
from loom.core.di.scope import Scope

__all__ = [
    "ConfiguredInstaller",
    "ContainerSealed",
    "Installer",
    "LoomContainer",
    "ResolutionError",
    "Scope",
]
