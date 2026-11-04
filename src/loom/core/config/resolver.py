"""Pluggable config resolver protocol.

Resolvers extend the YAML loader with custom ``${prefix:key}`` interpolation,
enabling secret values to be fetched from external stores (e.g. AWS SSM,
Azure Key Vault) at parse time without ever writing them to disk.

Register resolvers via :func:`~loom.core.config.loader.load_config`::

    cfg = load_config("s3://bucket/prod.yaml", resolvers=[MyResolver()])

YAML placeholders::

    storage:
      catalogs:
        main:
          token: ${myresolver:prod/databricks/token}

The resolver name becomes the placeholder prefix.  Values are resolved
when OmegaConf materialises the config — i.e. at job startup, so secret
rotation takes effect on the next run without redeployment.

Built-in resolvers
------------------
:class:`~loom.core.config.ssm.SsmResolver` is the bundled implementation
for AWS SSM Parameter Store.  Install ``loom-kernel[config-ssm]`` to use it::

    from loom.core.config import load_config, SsmResolver

    cfg = load_config("config/prod.yaml", resolvers=[SsmResolver()])
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class ConfigResolver(Protocol):
    """Protocol for pluggable config value resolvers.

    Implementors provide a *name* (used as the OmegaConf placeholder prefix)
    and a *resolve* callable that fetches the actual value at parse time.

    See :class:`~loom.core.config.ssm.SsmResolver` for the bundled AWS SSM
    implementation.  Custom resolvers only need to satisfy this two-member
    protocol::

        class VaultResolver:
            @property
            def name(self) -> str:
                return "vault"

            def resolve(self, key: str) -> object:
                return vault_client.read_secret(key)

        cfg = load_config("config/prod.yaml", resolvers=[VaultResolver()])
    """

    @property
    def name(self) -> str:
        """OmegaConf resolver prefix.

        Used as the placeholder prefix in YAML: ``${<name>:key}``.
        Must be unique across all registered resolvers.

        Returns:
            Resolver name string (e.g. ``"ssm"``, ``"keyvault"``).
        """
        ...

    def resolve(self, key: str) -> object:
        """Resolve *key* to its string value.

        Called by OmegaConf when materialising ``${<name>:key}``
        placeholders.  Runs at config parse time (job startup), so the
        returned value reflects the current state of the backing store.

        Args:
            key: Key portion of the placeholder after the prefix separator
                (e.g. ``"/prod/token"`` for ``${ssm:/prod/token}``).

        Returns:
            Resolved value. Typically a string, but may be a structured type
            when the resolver supports JSON navigation.
        """
        ...


def default_resolvers() -> tuple[ConfigResolver, ...]:
    """Return loom's built-in resolvers: ``secrets`` and ``ssm``.

    Both build their client lazily on the first resolution from the ``aws:``
    section of the configuration they resolve, as written, and the AWS SDK's
    default credential chain.

    Returns:
        A :class:`~loom.core.config.secrets.SecretsManagerResolver` followed
        by a :class:`~loom.core.config.ssm.SsmResolver`, both freshly built.
    """
    # Local imports keep this protocol module free of the AWS implementations.
    from loom.core.config.secrets import SecretsManagerResolver
    from loom.core.config.ssm import SsmResolver

    return (SecretsManagerResolver(), SsmResolver())


def merge_resolvers(
    explicit: Sequence[ConfigResolver], defaults: Sequence[ConfigResolver]
) -> tuple[ConfigResolver, ...]:
    """Return *explicit* followed by the *defaults* whose names are still free.

    A default is dropped when an explicit resolver takes its name or when a
    resolver with that name is already registered in OmegaConf; the latter
    is logged at DEBUG level.

    Args:
        explicit: Resolvers that keep their position and always win their
            name.
        defaults: Candidate resolvers appended after ``explicit`` when their
            name is not taken.

    Returns:
        ``explicit`` in order, followed by the kept ``defaults`` in order.
    """
    # Local import keeps omegaconf out of the import of ``loom.core.config``.
    from omegaconf import OmegaConf

    taken = {resolver.name for resolver in explicit}
    kept: list[ConfigResolver] = []
    for resolver in defaults:
        if resolver.name in taken:
            continue
        if OmegaConf.has_resolver(resolver.name):
            logger.debug(
                "config resolver %r already registered; loom default skipped", resolver.name
            )
            continue
        kept.append(resolver)
    return (*explicit, *kept)


def with_default_resolvers(explicit: Sequence[ConfigResolver] = ()) -> tuple[ConfigResolver, ...]:
    """Return *explicit* followed by loom's built-in resolvers whose names are free.

    Equivalent to ``merge_resolvers(explicit, default_resolvers())``; the
    factories use it to register ``secrets`` and ``ssm`` behind user resolvers.

    Args:
        explicit: User resolvers that keep their position and always win
            their name.

    Returns:
        ``explicit`` in order, followed by the built-in resolvers whose
        name is neither taken by ``explicit`` nor already registered.
    """
    return merge_resolvers(explicit, default_resolvers())


__all__ = ["ConfigResolver", "default_resolvers", "merge_resolvers", "with_default_resolvers"]
