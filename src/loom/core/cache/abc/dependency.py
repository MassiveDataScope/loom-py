from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from loom.core.stores.repository.mutation import MutationEvent


class DependencyResolver(Protocol):
    """Protocol for cache dependency tracking and invalidation."""

    async def fingerprint(self, tags: list[str]) -> str:
        """Compute a composite fingerprint from the current generation of each tag.

        Args:
            tags: Dependency tag names.

        Returns:
            A stable hash string representing the combined tag state.
        """
        ...

    async def bump_from_events(self, events: tuple[MutationEvent, ...]) -> None:
        """Increment generation counters for the tags affected by mutation events.

        A resolver implemented outside the framework keeps whatever
        granularity it implements; see
        :class:`~loom.core.cache.dependency.GenerationalDependencyResolver`
        for the built-in policy.

        Args:
            events: Mutation events produced within a transaction.
        """
        ...

    def entity_tags(self, entity: str, entity_id: object | None) -> list[str]:
        """Return dependency tags for a single entity lookup.

        Args:
            entity: Normalized entity name.
            entity_id: Primary key of the entity, or ``None``.

        Returns:
            List of tag names that should be tracked for this entity.
        """
        ...

    def list_tags(self, entity: str, filter_fingerprint: str) -> list[str]:
        """Return dependency tags for a list/index query.

        Args:
            entity: Normalized entity name.
            filter_fingerprint: Hash of the applied filter parameters.

        Returns:
            List of tag names that should be tracked for this list query.
        """
        ...


@runtime_checkable
class BatchFingerprintResolver(Protocol):
    """Optional capability: fingerprint several tag groups in one round trip.

    A :class:`DependencyResolver` that also implements this protocol is asked
    for every fingerprint of a page at once, which turns the per-entity
    counter lookups of a cached list into a single backend read.  Resolvers
    that do not implement it keep working through
    :meth:`DependencyResolver.fingerprint`, one call per group.

    Implementations must return exactly the same value ``fingerprint`` would
    return for each group, in the same order as ``tag_groups``.
    """

    async def fingerprint_many(self, tag_groups: Sequence[list[str]]) -> list[str]:
        """Compute one fingerprint per tag group.

        Args:
            tag_groups: Dependency tag names, grouped per entity.

        Returns:
            One fingerprint per group, in input order.
        """
        ...
