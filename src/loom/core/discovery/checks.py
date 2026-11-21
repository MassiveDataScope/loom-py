"""Deploy check of the discovery: ``loom.discovery.W001``, an entry-point group nothing provides.

A group of ``app.entry_points`` no installed distribution provides is a valid
state, so it is not an error. The check keeps a typo in a group name visible; a
group declared ``optional`` is not judged. It reads the groups as written and
resolves nothing; a group given by a placeholder, or that does not decode, is not
judged (the configuration load reports the latter).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from loom.core.checks import CheckSet, Finding
from loom.core.config.entry_points import EntryPointGroup
from loom.core.config.keys import INTERPOLATION, ConfigKey
from loom.core.discovery.scanner import PackageScanner

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

EMPTY_ENTRY_POINT_GROUP_CHECK: Final = "loom.discovery.W001"
"""Id of the check warning about an ``app.entry_points`` group with no installed entry point."""

CHECKS: Final = CheckSet()
"""Checks of ``app.entry_points``, run when the configuration declares it."""

_ENTRY_POINTS_KEY: Final = f"{ConfigKey.APP}.entry_points"


def _literal_groups(declared: list[object]) -> list[EntryPointGroup]:
    groups: list[EntryPointGroup] = []
    for item in declared:
        try:
            group = EntryPointGroup.from_declared(item)
        except ValueError:
            continue
        if INTERPOLATION not in group.name:
            groups.append(group)
    return groups


@CHECKS.register(EMPTY_ENTRY_POINT_GROUP_CHECK)
def _empty_entry_point_group(config: ConfigContext) -> Finding | None:
    declared = config.raw(_ENTRY_POINTS_KEY, [])
    literal_groups = _literal_groups(declared) if isinstance(declared, list) else []
    empty = PackageScanner(entry_points=literal_groups).empty_groups()
    if not empty:
        return None
    named = ", ".join(repr(group) for group in empty)
    return Finding(
        message=f"{_ENTRY_POINTS_KEY} group {named} has no installed entry point.",
        hint="install the distribution that provides it, fix the group name, or silence "
        "the check when the group is optional.",
    )


__all__ = ["CHECKS", "EMPTY_ENTRY_POINT_GROUP_CHECK"]
