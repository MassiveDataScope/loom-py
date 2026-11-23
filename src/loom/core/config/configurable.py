"""Declarative config bindings for runtime behaviour classes."""

from __future__ import annotations

import msgspec

from loom.core.errors import InvalidValue
from loom.core.model import LoomFrozenStruct


class ConfigBinding(LoomFrozenStruct, frozen=True):
    """Deferred binding between a class and a config section.

    The binding is a declaration only. It does not read files, access global
    config state, or instantiate ``target``. Compiler/bootstrap code resolves
    it later using an explicit runtime config object.

    Args:
        target: Runtime behaviour class to instantiate later.
        config_path: Dot-separated config path. Empty means no YAML section is
            required and only ``overrides`` are applied.
        overrides: Explicit keyword overrides. These win over YAML values when
            the binding is resolved.
    """

    target: type[object]
    config_path: str = ""
    overrides: dict[str, object] = msgspec.field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the config path."""
        if self.config_path and not self.config_path.strip():
            raise InvalidValue("ConfigBinding.config_path must not be blank.")


class Configurable:
    """Mixin for classes that can be declared from config paths.

    The mixin returns :class:`ConfigBinding` declarations. It does not impose a
    constructor shape and does not resolve configuration itself.
    """

    @classmethod
    def from_config(cls, config_path: str, **overrides: object) -> ConfigBinding:
        """Declare this class as configured from a YAML section.

        Args:
            config_path: Dot-separated config path resolved later by compiler
                or bootstrap code.
            **overrides: Explicit keyword overrides applied after YAML values.

        Returns:
            Deferred config binding for this class.

        Raises:
            InvalidValue: If ``config_path`` is empty or blank.
        """
        if not config_path.strip():
            raise InvalidValue("config_path must not be empty.")
        return ConfigBinding(
            target=cls,
            config_path=config_path,
            overrides=dict(overrides),
        )

    @classmethod
    def configure(cls, **overrides: object) -> ConfigBinding:
        """Declare this class with explicit keyword overrides only.

        Args:
            **overrides: Explicit keyword values used during resolution.

        Returns:
            Deferred config binding for this class.
        """
        return ConfigBinding(target=cls, overrides=dict(overrides))


__all__ = ["ConfigBinding", "Configurable"]
