"""The top-level ``aws:`` section: the client settings every AWS product of loom inherits.

A product's own section (a ``dynamodb`` store, ``storage.dynamodb``) overrides
it key by key. It decodes as :class:`~loom.core.connections.AwsClientConfig`,
which has no credential key: credentials never come from configuration.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

from loom.core.config.keys import ConfigKey
from loom.core.config.loader import convert_section
from loom.core.config.retired import reject_retired_keys
from loom.core.connections.aws import AwsClientConfig

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext


def aws_section(written: object) -> dict[str, Any]:
    """Return the ``aws:`` section *written*, once it decodes; ``{}`` when it is ``None``.

    Raises:
        ConfigError: When the section holds a retired or unknown key (a
            credential among them) or fails validation.
    """
    if written is None:
        return {}
    reject_retired_keys(written, ConfigKey.AWS, AwsClientConfig)
    convert_section(written, ConfigKey.AWS, AwsClientConfig)
    return dict(cast(Mapping[str, Any], written))


def aws_defaults(ctx: ConfigContext) -> dict[str, Any]:
    """Return the resolved ``aws:`` section of *ctx*: what a product section overrides key by key.

    Raises:
        ConfigError: As :func:`aws_section`.
    """
    return aws_section(ctx.section(ConfigKey.AWS, dict) if ctx.has(ConfigKey.AWS) else None)


__all__ = ["aws_defaults", "aws_section"]
