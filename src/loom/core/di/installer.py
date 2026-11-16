"""Application dependency installers."""

from __future__ import annotations

from abc import ABC, abstractmethod

from loom.core.di.container import LoomContainer


class Installer(ABC):
    """Register application dependencies in a container."""

    @abstractmethod
    def install(self, container: LoomContainer) -> None:
        """Register the dependencies provided by this installer."""
