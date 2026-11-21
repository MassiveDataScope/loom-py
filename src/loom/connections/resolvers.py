"""The resolvers loom registers for a configuration: the AWS ones and the core's own."""

from __future__ import annotations

from collections.abc import Sequence

from loom.connections.aws import SecretsManagerResolver, SsmResolver
from loom.core.config import ConfigResolver, default_resolvers, merge_resolvers


def built_in_resolvers() -> tuple[ConfigResolver, ...]:
    """Return ``secrets``, ``ssm`` and the core's default resolvers, freshly built.

    The AWS resolvers build their client lazily on the first resolution, from the
    ``aws:`` section of the configuration they resolve, as written, and the AWS SDK's
    default credential chain.
    """
    return (SecretsManagerResolver(), SsmResolver(), *default_resolvers())


def with_built_in_resolvers(
    explicit: Sequence[ConfigResolver] = (),
) -> tuple[ConfigResolver, ...]:
    """Return *explicit* followed by the built-in resolvers whose names are free.

    Args:
        explicit: User resolvers that keep their position and always win their name.

    Returns:
        ``explicit`` in order, followed by the built-in resolvers whose name is neither
        taken by ``explicit`` nor already registered.
    """
    return merge_resolvers(explicit, built_in_resolvers())


__all__ = ["built_in_resolvers", "with_built_in_resolvers"]
