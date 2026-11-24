"""The ``aws:`` section as the default of every ``AwsClientConfig`` a section holds."""

from __future__ import annotations

import types
from collections.abc import Mapping
from typing import TYPE_CHECKING, Annotated, Any, Union, get_args, get_origin

import msgspec

from loom.core.connections.aws.config import AwsClientConfig

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

_NONE_TYPE = type(None)
_UNIONS = (Union, types.UnionType)


class AwsInheritance:
    """Fills, key by key, every ``AwsClientConfig`` field written in a section from ``aws:``.

    A field typed :class:`AwsClientConfig` or a subclass, at any depth of
    structs, optional fields, lists and mappings, takes what it does not set
    from the section, exactly as the store plugins and the SQS transport do
    through :meth:`AwsClientConfig.inherit_from`. A field that is not written
    keeps its default.

    Args:
        aws: The ``aws:`` section, empty when there is none.
    """

    def __init__(self, aws: Mapping[str, Any]) -> None:
        self._aws = aws

    @classmethod
    def resolved(cls, ctx: ConfigContext) -> AwsInheritance:
        """The inheritance from the ``aws:`` section of *ctx*, its placeholders resolved."""
        from loom.core.config.keys import ConfigSection

        return cls(ctx.section(ConfigSection.AWS, dict) if ctx.has(ConfigSection.AWS) else {})

    @classmethod
    def as_written(cls, ctx: ConfigContext) -> AwsInheritance:
        """The inheritance from the ``aws:`` section of *ctx* as written, resolving nothing."""
        from loom.core.config.keys import ConfigSection

        written = ctx.raw(ConfigSection.AWS, {})
        return cls(written if isinstance(written, dict) else {})

    def apply(self, target: Any, written: Any, path: str) -> Any:
        """Return *written*, the data of a *target*, with every AWS client field filled in.

        Args:
            target: The type *written* decodes into.
            written: The section data; anything *target* does not describe is returned as is.
            path: Dotted path of *written*, for the messages.

        Returns:
            The data, a copy where a field was filled in.

        Raises:
            ConfigError: As :meth:`AwsClientConfig.inherit_from`.
        """
        origin = get_origin(target)
        if origin is Annotated:
            return self.apply(get_args(target)[0], written, path)
        if origin in _UNIONS:
            members = [member for member in get_args(target) if member is not _NONE_TYPE]
            return self.apply(members[0], written, path) if len(members) == 1 else written
        if origin is list and isinstance(written, list):
            (item,) = get_args(target)
            return [
                self.apply(item, value, f"{path}.{index}") for index, value in enumerate(written)
            ]
        if origin is dict and isinstance(written, dict):
            _, value_type = get_args(target)
            return {
                key: self.apply(value_type, value, f"{path}.{key}")
                for key, value in written.items()
            }
        if isinstance(target, type) and issubclass(target, msgspec.Struct):
            return self._apply_to_struct(target, written, path)
        return written

    def _apply_to_struct(self, target: type[msgspec.Struct], written: Any, path: str) -> Any:
        if not isinstance(written, dict):
            return written
        if issubclass(target, AwsClientConfig):
            return target.inherit_from(self._aws, written, path)
        fields = {field.encode_name: field.type for field in msgspec.structs.fields(target)}
        return {
            key: self.apply(fields[key], value, f"{path}.{key}") if key in fields else value
            for key, value in written.items()
        }


__all__ = ["AwsInheritance"]
