"""What each start-up path binds, and how many executors it constructs, on today's code.

Each snapshot ``bindings/<path>.json`` holds, per bound key, its scope and
provider qualname, the resolved type of an ``APPLICATION`` binding, the
container's model-to-repository mapping, the number of ``RuntimeExecutor``
constructions and which of them the container serves.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom.core.di.container import LoomContainer
from loom.core.engine.executor import RuntimeExecutor
from tests.unit.composition.pipeline import paths
from tests.unit.composition.pipeline.components import AcmeLedgerEntry, AcmeRecord
from tests.unit.composition.pipeline.snapshot import describe_container, matches_snapshot

_TWO_STORES = paths.Composition(
    models=(AcmeRecord, AcmeLedgerEntry),
    config={"stores": {"ledger": paths.SQLITE}},
)
_EVERY_PATH = {**paths.COMPOSED, "harness": paths.harness, **paths.STANDALONE}


def _recorded(container: LoomContainer, executors: list[RuntimeExecutor]) -> dict[str, Any]:
    served = container.resolve(RuntimeExecutor)
    return {
        **describe_container(container),
        "executors": {
            "constructed": len(executors),
            "served": [index for index, built in enumerate(executors) if built is served],
        },
    }


@pytest.mark.parametrize("name", list(_EVERY_PATH))
def test_each_path_binds_what_it_bound_before_the_pipeline(
    name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, executors: list[RuntimeExecutor]
) -> None:
    container = _EVERY_PATH[name](paths.Composition(), tmp_path, monkeypatch)

    matches_snapshot(f"bindings/{name}", _recorded(container, executors))


@pytest.mark.parametrize("name", list(paths.COMPOSED))
def test_each_composed_path_binds_a_second_store_as_before(
    name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, executors: list[RuntimeExecutor]
) -> None:
    container = paths.COMPOSED[name](_TWO_STORES, tmp_path, monkeypatch)

    matches_snapshot(f"bindings/{name}-two-stores", _recorded(container, executors))
