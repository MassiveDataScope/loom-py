"""An installer a test lists in ``app.packages`` as one module, to be found by the scan."""

from __future__ import annotations

from loom.core.di.container import LoomContainer
from loom.core.di.installer import Installer

MODULE = __name__
"""The entry that makes the scan find the installer below."""


class AcmeInstaller(Installer):
    def install(self, container: LoomContainer) -> None:
        del container
