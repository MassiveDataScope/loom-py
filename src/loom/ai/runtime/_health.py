"""Cached agent health and the worst-first aggregation of its checks.

The vocabulary is shared with the HTTP contract: a health value is what
``/health`` projects, so the aggregate of several dependencies is the worst of
them and never the average.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from types import MappingProxyType

from loom.ai.abc import HealthState
from loom.core.model import LoomFrozenStruct

"""Aggregate health vocabulary shared with the HTTP contract."""

_STATE_ORDER: Mapping[str, int] = MappingProxyType({"ok": 0, "degraded": 1, "unavailable": 2})
_STATE_BY_RANK: Mapping[int, HealthState] = MappingProxyType(
    {0: "ok", 1: "degraded", 2: "unavailable"}
)

_EMPTY_CHECKS: Mapping[str, str] = MappingProxyType({})


class AgentHealth(LoomFrozenStruct, frozen=True, kw_only=True):
    """Cached health of one agent and of its live dependencies.

    Attributes:
        status: Aggregate state, the worst of every check.
        checks: Per-dependency state, keyed ``"model"``, ``"mcp:<server>"``,
            ``"a2a:<agent>"`` or ``"sql:<connection>"``, always by the name the
            deployment registered rather than by URL. Internal topology: only
            an authenticated caller ever sees it.
        detail: Optional explanation, ``"probing"`` until the first probe of
            the background refresher completes.
    """

    status: HealthState
    checks: Mapping[str, str] = _EMPTY_CHECKS
    detail: str | None = None


def worst(states: Iterable[str]) -> HealthState:
    """Return the worst of several dependency states, ``"ok"`` when there are none."""
    ranks = (_STATE_ORDER.get(state, 2) for state in states)
    return _STATE_BY_RANK[max(ranks, default=0)]
