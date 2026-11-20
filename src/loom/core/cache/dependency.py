from __future__ import annotations

import asyncio
from collections.abc import Sequence

from loom.core.cache._batching import COUNTER_BATCH_SIZE, batched
from loom.core.cache.abc.dependency import BatchFingerprintResolver, DependencyResolver
from loom.core.cache.abc.store import CacheStore
from loom.core.cache.keys import stable_hash
from loom.core.stores.repository.mutation import MutationEvent


class GenerationalDependencyResolver(DependencyResolver, BatchFingerprintResolver):
    """Generational tags with monotonic counters in cache backend.

    A mutation bumps only the tags it affects (see :meth:`bump_from_events`),
    so updating one row leaves every other cached row of the same entity
    warm.  The bare ``entity`` tag stays in every key's tag list but the
    framework never bumps it: it is the manual entity-wide flush handle.  An
    operator increments ``tag:<entity>`` by hand to evict every key of that
    entity at once.
    """

    def __init__(self, cache: CacheStore) -> None:
        """Initialise the resolver with a cache backend for counter storage.

        Args:
            cache: Backend used to persist generation counters.
        """
        self._cache = cache

    def _tag_key(self, tag: str) -> str:
        return f"tag:{tag}"

    async def fingerprint(self, tags: list[str]) -> str:
        """Compute a composite fingerprint from generation counters of all tags.

        Args:
            tags: Dependency tag names.

        Returns:
            A stable hash representing the combined tag generation state.
        """
        if not tags:
            return "0"
        values = await self._cache.multi_get_values([self._tag_key(tag) for tag in tags], type=int)
        return self._compose(values)

    async def fingerprint_many(self, tag_groups: Sequence[list[str]]) -> list[str]:
        """Compute one fingerprint per tag group, reading every counter once.

        The distinct tags of every group are read together, in batches, so the
        cost is one round trip per
        :data:`~loom.core.cache._batching.COUNTER_BATCH_SIZE` distinct tags
        instead of one per group.

        Args:
            tag_groups: Dependency tag names, grouped per entity.

        Returns:
            One fingerprint per group, in input order; each value is identical
            to what :meth:`fingerprint` returns for the same group.
        """
        unique_tags = list(dict.fromkeys(tag for tags in tag_groups for tag in tags))
        if not unique_tags:
            return ["0" for _ in tag_groups]
        counters = await self._read_counters(unique_tags)
        return [self._compose([counters.get(tag) for tag in tags]) for tags in tag_groups]

    async def _read_counters(self, tags: Sequence[str]) -> dict[str, int | None]:
        batches = list(batched(tags, COUNTER_BATCH_SIZE))
        results = await asyncio.gather(
            *(
                self._cache.multi_get_values([self._tag_key(tag) for tag in batch], type=int)
                for batch in batches
            )
        )
        return {
            tag: value
            for batch, values in zip(batches, results, strict=True)
            for tag, value in zip(batch, values, strict=True)
        }

    @staticmethod
    def _compose(counters: Sequence[int | None]) -> str:
        """Hash generation counters in tag order; the only definition of the rule."""
        if not counters:
            return "0"
        return stable_hash("|".join("0" if value is None else str(value) for value in counters))

    async def bump_from_events(self, events: tuple[MutationEvent, ...]) -> None:
        """Increment generation counters for the tags affected by mutation events.

        Every event bumps ``entity:list`` and ``entity:id:<k>`` per id; the
        event's own ``tags`` are bumped as they come.  The bare ``entity`` tag
        is never bumped by the framework: it is reserved for a manual
        entity-wide flush.

        The counters are bumped concurrently rather than one round trip at a
        time: with K distinct tags, a sequential loop pays K round trips in
        series, while awaiting them together pays roughly one round trip
        regardless of K once the backend is network-bound (see the benchmark
        referenced in the PR that introduced this).

        Args:
            events: Mutation events to process.
        """
        bump_keys: set[str] = set()
        for event in events:
            bump_keys.add(self._tag_key(f"{event.entity}:list"))
            for entity_id in event.ids:
                bump_keys.add(self._tag_key(f"{event.entity}:id:{entity_id}"))
            for tag in event.tags:
                bump_keys.add(self._tag_key(tag))
        await asyncio.gather(*(self._cache.incr(key, delta=1) for key in bump_keys))

    def entity_tags(self, entity: str, entity_id: object | None) -> list[str]:
        """Return dependency tags for a single entity lookup.

        Args:
            entity: Normalized entity name.
            entity_id: Primary key of the entity, or ``None``.

        Returns:
            List of tag names for this entity.
        """
        tags = [entity]
        if entity_id is not None:
            tags.append(f"{entity}:id:{entity_id}")
        return tags

    def list_tags(self, entity: str, filter_fingerprint: str) -> list[str]:
        """Return dependency tags for a list/index query.

        Args:
            entity: Normalized entity name.
            filter_fingerprint: Hash of the applied filter parameters.

        Returns:
            List of tag names for this list query.
        """
        return [entity, f"{entity}:list", f"{entity}:list:filters:{filter_fingerprint}"]
