"""Application dependency installers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Generic, Self, TypeVar

from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer

SettingsT = TypeVar("SettingsT")


class Installer(ABC):
    """Register application dependencies in a container."""

    @classmethod
    def from_config(cls, config: ConfigContext) -> Self:
        """Build the installer composition runs; the default takes no configuration.

        Args:
            config: The application configuration.

        Returns:
            The installer, built once per composition.
        """
        del config
        return cls()

    @abstractmethod
    def install(self, container: LoomContainer) -> None:
        """Register the dependencies provided by this installer."""


class ConfiguredInstaller(Installer, Generic[SettingsT]):
    """An installer with its own section of the application configuration.

    Declare where the section lives and what it decodes into; composition and
    ``loom check`` decode it the same way and hand it to ``__init__``::

        class StoresInstaller(ConfiguredInstaller[StoresSettings]):
            section = "trachinus.stores"
            settings_type = StoresSettings

            def install(self, container: LoomContainer) -> None:
                container.register(Bucket, lambda: Bucket(self.settings.bucket))

    Args:
        settings: The decoded section.
    """

    section: ClassVar[str]
    settings_type: ClassVar[type[Any]]

    def __init__(self, settings: SettingsT) -> None:
        self.settings: SettingsT = settings

    @classmethod
    def from_config(cls, config: ConfigContext) -> Self:
        """Decode the section into ``settings_type`` and build the installer with it.

        Raises:
            ConfigError: When the section is missing or fails validation; the
                message names the section and the installer.
        """
        try:
            settings = config.section(cls.section, cls.settings_type)
        except ConfigError as error:
            raise ConfigError(
                f"{cls.__qualname__} reads the {cls.section!r} section: {error}"
            ) from error
        return cls(settings)
