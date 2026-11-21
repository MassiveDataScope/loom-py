from __future__ import annotations

from typing import Any, cast

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Load, RelationshipProperty, selectinload


class ProfileLoader:
    """Resolve model relationship profiles into SQLAlchemy load options."""

    _cache: dict[type[Any], dict[str, list[Load]]] = {}

    @staticmethod
    def get_options(entity_class: type[Any], profile: str) -> list[Load]:
        by_profile = ProfileLoader._cache.get(entity_class)
        if by_profile is None:
            by_profile = ProfileLoader._build_profile_map(entity_class)
            ProfileLoader._cache[entity_class] = by_profile

        if profile not in by_profile:
            raise ValueError(f"Profile '{profile}' not found for {entity_class.__name__}")
        return by_profile[profile]

    @staticmethod
    def _build_profile_map(entity_class: type[Any]) -> dict[str, list[Load]]:
        mapper = sa_inspect(entity_class)
        profile_map: dict[str, list[Load]] = {"default": []}
        for relationship in mapper.relationships:
            option = cast(Load, selectinload(getattr(entity_class, relationship.key)))
            for profile in ProfileLoader._relationship_profiles(relationship):
                profile_map.setdefault(profile, []).append(option)
        return profile_map

    @staticmethod
    def _relationship_profiles(relationship: RelationshipProperty[Any]) -> tuple[str, ...]:
        raw_profiles = (relationship.info or {}).get("profiles", ("default",))
        if isinstance(raw_profiles, str):
            return (raw_profiles,)
        return cast(tuple[str, ...], tuple(raw_profiles))
