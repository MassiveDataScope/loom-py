"""An installer that binds ``AwsClients``, a key composition owns, to be found by the scan."""

from __future__ import annotations

from loom.connections.aws import AwsClients
from loom.core.di.container import LoomContainer
from loom.core.di.installer import Installer

MODULE = __name__
"""The entry that makes the scan find the installer below."""


class AcmeAwsInstaller(Installer):
    def install(self, container: LoomContainer) -> None:
        container.register_instance(AwsClients, AwsClients())
