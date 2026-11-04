"""The top-level ``aws:`` section: the client settings every AWS product of loom inherits.

A product's own section (a ``dynamodb`` store, ``storage.dynamodb``) overrides
it key by key. It takes the keys of :class:`~loom.core.connections.AwsClientConfig`
only, which has no credential key: credentials never come from configuration.
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
    """Return the resolved ``aws:`` section of *ctx*, or ``None`` when it declares none."""
    return ctx.section(ConfigKey.AWS, dict) if ctx.has(ConfigKey.AWS) else None


def inherit_aws(
    aws: object, section: Mapping[str, Any], path: str, target: type[AwsClientConfig]
) -> dict[str, Any]:
    """Return the product *section* at *path*, taking every key it does not set from *aws*.

    Raises:
        ConfigError: When *aws* (``None`` when absent) is not a mapping or
            holds a key :class:`AwsClientConfig` does not take, a credential
            among them, or *aws* or *section* (for *target*) a retired key.
    """
    if aws is not None and not isinstance(aws, Mapping):
        raise ConfigError(f"Config section {ConfigKey.AWS.value!r} must be a mapping")
    reject_retired_keys(aws, ConfigKey.AWS, AwsClientConfig)
    reject_retired_keys(section, path, target)
    inherited: dict[str, Any] = dict(aws) if isinstance(aws, Mapping) else {}
    unknown = sorted(set(inherited) - set(AwsClientConfig.__struct_encode_fields__))
    if unknown:
        raise ConfigError(f"Config section {ConfigKey.AWS.value!r} takes no key {unknown[0]!r}")
    return {**inherited, **section}


def decode_aws(
    aws: object,
    section: Mapping[str, Any],
    path: str,
    target: type[ClientConfigT],
    **overrides: Any,
) -> ClientConfigT:
    """Decode :func:`inherit_aws` of *section*, then *overrides*, into *target*, once.

    Raises:
        ConfigError: As :func:`inherit_aws`, or naming *path* when it fails validation.
    """
    return convert_section(inherit_aws(aws, section, path, target) | overrides, path, target)


__all__ = ["decode_aws", "inherit_aws", "written_aws"]
