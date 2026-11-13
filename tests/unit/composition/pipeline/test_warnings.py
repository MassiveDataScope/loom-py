"""The warnings ``HttpTestHarness.build_app`` emits on today's code: the baseline T01e keeps.

``snapshots/warnings/harness.json`` lists each warning's category and
message, every filter set to ``always``, in the order emitted.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.unit.composition.pipeline import paths
from tests.unit.composition.pipeline.snapshot import (
    describe_warnings,
    matches_snapshot,
    recorded_warnings,
)


def test_the_harness_builds_with_the_warnings_it_emitted_before(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with recorded_warnings() as caught:
        paths.harness(paths.Composition(), tmp_path, monkeypatch)

    matches_snapshot("warnings/harness", describe_warnings(caught))
