"""Deploy check of the discovery: ``loom.discovery.W001``, an entry-point group nothing provides.

A group of ``app.entry_points`` no installed distribution provides is a valid
state for an optional plugin group, so it is not an error. The check keeps a
typo in a group name visible. It reads the groups as written and resolves
nothing; a group given by a placeholder is not judged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from loom.core.checks import CheckSet, Finding
from loom.core.config.keys import INTERPOLATION, ConfigKey
from loom.core.discovery.scanner import PackageScanner

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

EMPTY_ENTRY_POINT_GROUP_CHECK: Final = "loom.discovery.W001"
"""Id of the check warning about an ``app.entry_points`` group with no installed entry point."""

CHECKS: Final = CheckSet()
"""Checks of ``app.entry_points``, run when the configuration declares it."""

_ENTRY_POINTS_KEY: Final = f"{ConfigKey.APP}.entry_points"


@CHECKS.register(EMPTY_ENTRY_POINT_GROUP_CHECK)
def _empty_entry_point_group(config: ConfigContext) -> Finding | None:
    declared = config.raw(_ENTRY_POINTS_KEY, [])
    literal_groups = (
        [group for group in declared if isinstance(group, str) and INTERPOLATION not in group]
        if isinstance(declared, list)
        else []
    )
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
