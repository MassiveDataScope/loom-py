from __future__ import annotations

import hashlib
from typing import Any

import msgspec


def stable_hash(value: str) -> str:
    """Produce a short, deterministic hash string from the given input.

    Args:
        value: Arbitrary string to hash.

    Returns:
        A 16-character hexadecimal SHA-256 digest prefix.
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def full_hash(value: str) -> str:
    """Produce the full SHA-256 hex digest of *value*.

    Used instead of :func:`stable_hash` wherever the input is chosen by a
    caller — a filter, a query, a scope — since two such inputs sharing a
    digest would share a cache entry.

    Args:
        value: Arbitrary string to hash.

    Returns:
        The 64-character hexadecimal SHA-256 digest.
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _scope_digest(scope: str) -> str:
    """Render a caller scope for a cache key: ``""`` stays ``""``, any other value is hashed.

    Hashing keeps the scope's values, such as a tenant id, out of key names,
    which the cache logs.
    """
    return full_hash(scope) if scope else ""


def entity_key(
    entity: str,
    entity_id: object,
    profile: str,
    deps_fingerprint: str,
    *,
    scope: str,
) -> str:
    """Build the cache key for a single entity lookup.

    Args:
        entity: Normalized entity name.
        entity_id: Primary key of the entity.
        profile: Loading profile name (e.g. ``"default"``).
        deps_fingerprint: Dependency fingerprint for generational invalidation.
        scope: The caller's row-level-security scope, ``""`` when unscoped;
            a row cached for one scope is never served to another.

    Returns:
        A composite cache key string.
    """
    return (
        f"{entity}:{entity_id}:profile={profile}:deps={deps_fingerprint}"
        f":scope={_scope_digest(scope)}"
    )


def list_index_key(
    entity: str,
    filter_fingerprint: str,
    page: int,
    limit: int,
    profile: str,
    deps_fingerprint: str,
    *,
    scope: str,
) -> str:
    """Build the cache key for a paginated list/index query.

    Args:
        entity: Normalized entity name.
        filter_fingerprint: Hash of the applied filter parameters.
        page: Page number (1-based).
        limit: Maximum number of items per page.
        profile: Loading profile name.
        deps_fingerprint: Dependency fingerprint for generational invalidation.
        scope: The caller's row-level-security scope, ``""`` when unscoped;
            a page of ids cached for one scope is never served to another.

    Returns:
        A composite cache key string.
    """
    return (
        f"{entity}:list:filters={filter_fingerprint}:page={page}:limit={limit}:"
        f"profile={profile}:deps={deps_fingerprint}:scope={_scope_digest(scope)}"
    )


def call_key(*, module: str, qualname: str, version: int, arguments: Any) -> str:
    """Build the cache key for one call to a ``@cache_call`` coroutine.

    The digest is the **full** SHA-256 of the canonically rendered arguments,
    not :func:`stable_hash`: the arguments of a cached call are often chosen by
    a model or an end user, and 64 bits over a space someone else influences is
    not the problem an internal filter fingerprint solves.

    Args:
        module: ``__module__`` of the decorated coroutine, so two homonyms in
            different modules never share an entry.
        qualname: ``__qualname__`` of the decorated coroutine.
        version: Version declared by the caller, the only manual invalidation
            a cached call has.
        arguments: Canonical rendering of the bound arguments, already made
            order-independent by the caller.

    Returns:
        A composite cache key string.

    Raises:
        TypeError: The rendering contains something JSON cannot encode. The
            call renderer no longer produces one — it emits nested lists of
            JSON scalars, and a mapping as pairs rather than as a ``dict`` —
            but the guard belongs to any caller that builds its own rendering.
        msgspec.EncodeError: The rendering cannot be encoded at all.
    """
    digest = hashlib.sha256(msgspec.json.encode(arguments)).hexdigest()
    return f"call:{module}.{qualname}:v{version}:{digest}"
