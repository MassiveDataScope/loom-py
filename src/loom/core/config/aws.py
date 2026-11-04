"""The top-level ``aws:`` section: the client keys every AWS product inherits, never credentials.

A product's own section (a ``dynamodb`` store, ``storage.dynamodb``) overrides it key by key.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from loom.core.config.errors import ConfigError
from loom.core.config.keys import ConfigKey
from loom.core.config.loader import convert_section
from loom.core.config.retired import reject_retired_keys
from loom.core.connections.aws import AwsClientConfig

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

ClientConfigT = TypeVar("ClientConfigT", bound=AwsClientConfig)


def written_aws(ctx: ConfigContext) -> object:
    return ctx.section(ConfigKey.AWS, dict) if ctx.has(ConfigKey.AWS) else None


def inherit_aws(
    aws: object, own: Mapping[str, Any], path: str, target: type[AwsClientConfig] = AwsClientConfig
) -> dict[str, Any]:
    """*own* section with the keys it does not set from *aws*; refuses retired and foreign keys."""
    if not isinstance(aws, Mapping | None):
        raise ConfigError(f"Config section {ConfigKey.AWS.value!r} must be a mapping")
    reject_retired_keys(aws, ConfigKey.AWS, AwsClientConfig)
    reject_retired_keys(own, path, target)
    unknown = sorted(set(aws or {}) - set(AwsClientConfig.__struct_encode_fields__))
    if unknown:
        raise ConfigError(f"Config section {ConfigKey.AWS.value!r} takes no key {unknown[0]!r}")
    return {**(aws or {}), **own}


def decode_aws(
    aws: object, section: Mapping[str, Any], path: str, target: type[ClientConfigT], **over: Any
) -> ClientConfigT:
    """:func:`inherit_aws`, then *over*, decoded once into *target*."""
    return convert_section(inherit_aws(aws, section, path, target) | over, path, target)


__all__ = ["decode_aws", "inherit_aws", "written_aws"]
