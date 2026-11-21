"""Every log event loom declares is named ``<area>.<subject>_<fact>`` and names one thing."""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from loom.core.logger import LogEvent
from tests.helpers.source_tree import source_files

_SRC = Path(__file__).resolve().parents[2] / "src"


def _loom_events() -> list[LogEvent]:
    for path in source_files():
        if "(LogEvent)" in path.read_text():
            module = ".".join(path.relative_to(_SRC).with_suffix("").parts)
            try:
                importlib.import_module(module)
            except ModuleNotFoundError as missing:
                pytest.skip(f"{module} needs {missing.name}")
    pending, found = [LogEvent], []
    while pending:
        base = pending.pop()
        pending.extend(base.__subclasses__())
        if base.__module__.startswith("loom."):
            found.extend(base)
    return found


class TestLoomLogEvents:
    def test_loom_declares_events(self) -> None:
        assert _loom_events()

    def test_every_member_follows_area_subject_fact(self) -> None:
        misnamed = [event for event in _loom_events() if not event.is_well_formed]

        assert misnamed == []

    def test_no_value_names_two_events(self) -> None:
        values = [event.value for event in _loom_events()]

        assert len(values) == len(set(values))


class TestEventForm:
    @pytest.mark.parametrize(
        "value", ["job.delivery_abandoned", "observability.lifecycle_started", "a.b_c_d"]
    )
    def test_a_name_in_the_form_is_well_formed(self, value: str) -> None:
        class Sample(LogEvent):
            EVENT = value

        assert Sample.EVENT.is_well_formed

    @pytest.mark.parametrize(
        "value",
        ["JobAbandoned", "job_abandoned", "job.abandoned", "Job.delivery_abandoned", "[BOOT] x"],
    )
    def test_a_name_outside_the_form_is_not(self, value: str) -> None:
        class Sample(LogEvent):
            EVENT = value

        assert not Sample.EVENT.is_well_formed
