"""Shared entry-point loader for Loom plugin groups.

This module is a stdlib-only leaf: it must never import from ``loom`` or from
any third-party package, so that ETL-only deployments can rely on it without
pulling optional dependencies into the process.

It centralises the plugin resolution policy used across pillars: duplicate
handling, missing-plugin reporting and the optional API-version handshake.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from importlib.metadata import Distribution, EntryPoint, entry_points
from typing import Literal

_log = logging.getLogger(__name__)

_UNKNOWN_DISTRIBUTION = "<unknown distribution>"

DuplicatePolicy = Literal["error", "warn_first"]
"""Policy applied when several distributions register the same plugin name."""


class EntryPointError(Exception):
    """Base error for every entry-point resolution failure."""


class EntryPointNotFoundError(EntryPointError):
    """Raised when no entry point matches the requested group and name.

    Attributes:
        group: Group that was scanned.
        name: Name that was requested.
        available: Names registered in ``group``, sorted and deduplicated, so a
            host can report the options without scanning the group again.
    """

    def __init__(self, group: str, name: str, available: tuple[str, ...]) -> None:
        self.group = group
        self.name = name
        self.available = available
        known = ", ".join(available) if available else "none"
        super().__init__(
            f"No entry point named {name!r} registered in group {group!r}. Available: {known}."
        )


class DuplicateEntryPointError(EntryPointError):
    """Raised when several distributions register the same group and name.

    Attributes:
        group: Group that was scanned.
        name: Name claimed more than once.
        distributions: Distributions claiming ``name``, in registration order,
            so a host can report every claimant without scanning again.
    """

    def __init__(self, group: str, name: str, distributions: tuple[str, ...]) -> None:
        self.group = group
        self.name = name
        self.distributions = distributions
        super().__init__(
            f"Entry point {name!r} in group {group!r} is registered by several "
            f"distributions: {', '.join(distributions)}."
        )


class ApiVersionMismatchError(EntryPointError):
    """Raised when a plugin object declares an unsupported API version.

    Attributes:
        declared: Value read from the handshake attribute, ``None`` when the
            attribute is absent, so a host can report what it saw.
        requirement: Contract the value was checked against.
    """

    def __init__(self, obj: object, declared: object, requirement: ApiVersionRequirement) -> None:
        self.declared = declared
        self.requirement = requirement
        supported = ", ".join(str(version) for version in sorted(requirement.supported))
        super().__init__(
            f"Plugin {obj!r} declares {requirement.attribute}={declared!r}, "
            f"which is not supported. Supported versions: {supported}."
        )


@dataclass(frozen=True, slots=True)
class ApiVersionRequirement:
    """Handshake contract a loaded plugin object must satisfy.

    Attributes:
        attribute: Name of the attribute declaring the plugin API version.
        supported: API versions the host accepts.
    """

    attribute: str
    supported: frozenset[int]


def check_api_version(obj: object, requirement: ApiVersionRequirement) -> None:
    """Check the handshake attribute of an already-obtained plugin object.

    Callers that must construct the plugin before the handshake can be read —
    an entry point targeting a class that declares its version on the instance
    — apply the contract here instead of at load time.

    Args:
        obj: Plugin object to inspect.
        requirement: Handshake contract to apply.

    Raises:
        ApiVersionMismatchError: When the attribute is absent, is not an
            integer, or names an unsupported version.
    """
    declared = getattr(obj, requirement.attribute, None)
    # ``type is int`` rather than isinstance: a bool would be a mistake, not a
    # version.
    if type(declared) is int and declared in requirement.supported:
        return
    raise ApiVersionMismatchError(obj, declared, requirement)


def list_entry_points(group: str) -> tuple[EntryPoint, ...]:
    """List every entry point registered under ``group``.

    Hosts use this to report what is installed (for example in a
    "plugin not found, available: ..." message) against exactly the same
    registrations `select_entry_point` resolves from.

    Args:
        group: Entry-point group to scan.

    Returns:
        The registered entry points, possibly empty.
    """
    return _iter_group(group)


def select_entry_point(
    group: str,
    name: str,
    *,
    on_duplicate: DuplicatePolicy,
) -> EntryPoint | None:
    """Select the entry point registered under ``group`` with ``name``.

    Args:
        group: Entry-point group to scan.
        name: Entry-point name to match.
        on_duplicate: ``"error"`` to reject conflicts, ``"warn_first"`` to log a
            warning and keep the first registration.

    Returns:
        The matching entry point, or ``None`` when no registration matches.

    Raises:
        DuplicateEntryPointError: When several distributions register ``name``
            in ``group`` and ``on_duplicate`` is ``"error"``.
    """
    matches = tuple(ep for ep in _iter_group(group) if ep.name == name)
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]
    return _resolve_conflict(group, name, matches, on_duplicate)


def load_entry_point(
    group: str,
    name: str,
    *,
    on_duplicate: DuplicatePolicy,
    api_version: ApiVersionRequirement | None = None,
) -> object:
    """Load the object exposed by the entry point ``name`` in ``group``.

    Args:
        group: Entry-point group to scan.
        name: Entry-point name to match.
        on_duplicate: Duplicate-name policy forwarded to `select_entry_point`.
        api_version: Optional handshake contract. When omitted, the loaded
            object is returned without any version validation.

    Returns:
        The object referenced by the entry point.

    Raises:
        EntryPointNotFoundError: When no registration matches.
        DuplicateEntryPointError: When duplicates are rejected by the policy.
        ApiVersionMismatchError: When the handshake is requested and the loaded
            object declares a missing or unsupported version.
    """
    ep = select_entry_point(group, name, on_duplicate=on_duplicate)
    if ep is None:
        raise EntryPointNotFoundError(group, name, _registered_names(group))
    obj: object = ep.load()
    if api_version is not None:
        check_api_version(obj, api_version)
    return obj


def _iter_group(group: str) -> tuple[EntryPoint, ...]:
    return tuple(entry_points().select(group=group))


def _resolve_conflict(
    group: str,
    name: str,
    matches: tuple[EntryPoint, ...],
    on_duplicate: DuplicatePolicy,
) -> EntryPoint:
    distributions = tuple(_distribution_name(ep) for ep in matches)
    if on_duplicate == "error":
        raise DuplicateEntryPointError(group, name, distributions)
    first = matches[0]
    _log.warning(
        "Entry point %r in group %r is registered by several distributions: %s. "
        "Using the first one, from %s.",
        name,
        group,
        distributions,
        _distribution_name(first),
    )
    return first


def _registered_names(group: str) -> tuple[str, ...]:
    return tuple(sorted({ep.name for ep in _iter_group(group)}))


def _distribution_name(ep: EntryPoint) -> str:
    dist: Distribution | None = getattr(ep, "dist", None)
    if dist is None:
        return _UNKNOWN_DISTRIBUTION
    return dist.name


__all__ = [
    "ApiVersionMismatchError",
    "ApiVersionRequirement",
    "DuplicateEntryPointError",
    "DuplicatePolicy",
    "EntryPointError",
    "EntryPointNotFoundError",
    "check_api_version",
    "list_entry_points",
    "load_entry_point",
    "select_entry_point",
]
