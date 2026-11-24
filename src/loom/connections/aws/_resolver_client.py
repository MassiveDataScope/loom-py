"""The AWS client a config resolver builds from the ``aws:`` section as written."""

from __future__ import annotations

from typing import Any, cast

from loom.core.config.context import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.config.keys import INTERPOLATION, ConfigSection
from loom.core.connections.aws import AwsClientConfig, AwsClientSource
from loom.core.plugins.optional import MissingExtraError


def build_resolver_client(
    clients: AwsClientSource, service: str, region: str | None, *, resolver: str, root: Any
) -> Any:
    """Build one boto3 client through the AWS island from the ``aws:`` section as written.

    The resolvers are the ones resolving the placeholders, so they read
    ``aws:`` of the configuration *root* unresolved; *region*, when given,
    overrides its region.

    Args:
        clients: The AWS clients the resolver owns.
        service: Service name, ``"ssm"`` or ``"secretsmanager"``.
        region: AWS region, or ``None`` to take ``aws.region`` or let boto3
            resolve its own.
        resolver: Resolver class name, for the messages.
        root: The configuration being resolved, or ``None`` outside one.

    Returns:
        The client; typed ``Any`` because boto3 ships no stubs.

    Raises:
        ConfigError: When boto3 is not installed, naming the extra, or
            ``aws:`` is invalid or holds a placeholder.
    """
    written = None
    if root is not None:
        from omegaconf import OmegaConf

        aws = OmegaConf.to_container(OmegaConf.masked_copy(root, ConfigSection.AWS), resolve=False)
        written = cast("dict[str, object]", aws).get(ConfigSection.AWS)
    for key, value in written.items() if isinstance(written, dict) else ():
        if isinstance(value, str) and INTERPOLATION in value:
            raise ConfigError(
                f"{ConfigSection.AWS}.{key} is a placeholder, but {resolver} reads "
                f"{ConfigSection.AWS} as written: write the value, or leave it out for boto3 "
                "to read its environment (AWS_REGION, AWS_ENDPOINT_URL)"
            )
    overrides: dict[str, Any] = {} if region is None else {"region": region}
    ctx = ConfigContext.from_dict({ConfigSection.AWS: written})
    config = AwsClientConfig.from_config(ctx, ConfigSection.AWS, **overrides)
    try:
        return clients.client(service, config)
    except MissingExtraError as exc:
        raise ConfigError(
            f"boto3 is required for {resolver}. Install it with: pip install loom-kernel[aws]"
        ) from exc


__all__ = ["build_resolver_client"]
