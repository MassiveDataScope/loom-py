"""Application dependency installers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Generic, Self, TypeGuard, TypeVar

import msgspec

from loom.core.config import ConfigContext, ConfigError
from loom.core.config.keys import INTERPOLATION
from loom.core.config.loader import config_strings, convert_section
from loom.core.connections.aws import AwsInheritance
from loom.core.di.container import LoomContainer
from loom.core.resources import Resource

SettingsT = TypeVar("SettingsT")


def _holds_placeholder(written: object) -> bool:
    return any(INTERPOLATION in text for _, text in config_strings(written))


def _is_struct_type(target: object) -> TypeGuard[type[msgspec.Struct]]:
    return isinstance(target, type) and issubclass(target, msgspec.Struct)


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

    @classmethod
    def check_config(cls, config: ConfigContext) -> None:
        """Validate, as written, what this installer reads from *config*; the default reads nothing.

        ``loom check`` calls it in place of :meth:`from_config`: it resolves no
        placeholder, so it runs in CI without the application's secrets.

        Raises:
            ConfigError: When what the installer reads is invalid as written.
        """
        del config

    @abstractmethod
    def install(self, container: LoomContainer) -> None:
        """Register the dependencies provided by this installer."""

    def resources(self, container: LoomContainer) -> tuple[Resource, ...]:
        """Declare what the dependencies this installer registered need opened and closed.

        Composition calls it once, after the container is sealed, and the
        application opens the resources after the stores and before the job
        timer, closing them in reverse. They get the guarantees of loom's own:
        a failure while opening closes what already opened, every one closes
        even when another fails, ``check`` runs right before the resource
        opens and in ``Application.check()``, and ``readiness`` answers the
        health probe. The default declares none.

        Args:
            container: The sealed container, to resolve what this installer registered.

        Returns:
            The resources, each unique by ``kind:name`` and by readiness key
            among all of the application's.
        """
        del container
        return ()


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

        A field of the settings typed ``AwsClientConfig``, at any depth, takes
        the keys it does not set from the top-level ``aws:`` section, as loom's
        own store plugins do.

        Raises:
            ConfigError: When the section is missing or fails validation; the
                message names the section and the installer.
        """
        return cls(cls._decode(config))

    @classmethod
    def check_config(cls, config: ConfigContext) -> None:
        """Validate the section as written, resolving no placeholder.

        The ``aws:`` section, as written, fills the fields typed
        ``AwsClientConfig`` first, as :meth:`from_config` does. A section with
        no placeholder is decoded as :meth:`from_config` decodes it, with the
        same error. A section holding placeholders is
        judged part by part: every value without a placeholder is decoded
        into its field's type as start-up decodes it, and a settings struct
        holding a placeholder somewhere below is judged on its keys (no key
        the settings forbid, no required field missing) and then field by
        field. A placeholder value itself is not type-checked, and neither
        is a list, a mapping or an optional struct that holds one:
        start-up judges them once resolved.

        Raises:
            ConfigError: As :meth:`from_config`, naming the section and the installer.
        """
        written = cls._inheriting(AwsInheritance.as_written(config), config.raw(cls.section))
        if _holds_placeholder(written):
            cls._check_written(cls.settings_type, written, cls.section)
        else:
            cls._decode(config)

    @classmethod
    def _decode(cls, config: ConfigContext) -> SettingsT:
        try:
            written = AwsInheritance.resolved(config).apply(
                cls.settings_type, config.section(cls.section, Any), cls.section
            )
        except ConfigError as error:
            raise cls._error(str(error)) from error
        return cls._convert(written)

    @classmethod
    def _inheriting(cls, inheritance: AwsInheritance, written: object) -> Any:
        try:
            return inheritance.apply(cls.settings_type, written, cls.section)
        except ConfigError as error:
            raise cls._error(str(error)) from error

    @classmethod
    def _convert(cls, written: object) -> SettingsT:
        try:
            settings: SettingsT = convert_section(written, cls.section, cls.settings_type)
        except ConfigError as error:
            raise cls._error(str(error)) from error
        return settings

    @classmethod
    def _check_written(cls, target: Any, written: object, key: str) -> None:
        if not _holds_placeholder(written):
            try:
                convert_section(written, key, target)
            except ConfigError as error:
                raise cls._error(str(error)) from error
            return
        if isinstance(written, dict) and _is_struct_type(target):
            cls._check_written_fields(target, written, key)

    @classmethod
    def _check_written_fields(
        cls, target: type[msgspec.Struct], written: dict[str, Any], key: str
    ) -> None:
        fields = msgspec.structs.fields(target)
        names = {field.encode_name for field in fields}
        unknown = (
            sorted(set(written) - names) if target.__struct_config__.forbid_unknown_fields else []
        )
        missing = [
            field.encode_name
            for field in fields
            if field.required and field.encode_name not in written
        ]
        if unknown or missing:
            raise cls._error(f"{key}: unknown keys {unknown}, missing required keys {missing}")
        for field in fields:
            if field.encode_name in written:
                cls._check_written(
                    field.type, written[field.encode_name], f"{key}.{field.encode_name}"
                )

    @classmethod
    def _error(cls, reason: str) -> ConfigError:
        return ConfigError(f"{cls.__qualname__} reads the {cls.section!r} section: {reason}")
