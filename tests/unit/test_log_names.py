"""Every log event and key loom declares is well formed and names one thing."""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from loom.core.log_names import LogEvent, LogField, LogKey
from tests.helpers.log_standard import loom_members
from tests.helpers.source_tree import source_files

_SRC = Path(__file__).resolve().parents[2] / "src"
_DECLARATIONS = ("(LogEvent)", "(LogKey)")


def _import_declaring_modules() -> None:
    for path in source_files():
        if any(marker in path.read_text() for marker in _DECLARATIONS):
            module = ".".join(path.relative_to(_SRC).with_suffix("").parts)
            try:
                importlib.import_module(module)
            except ModuleNotFoundError as missing:
                pytest.skip(f"{module} needs {missing.name}")


def _loom_events() -> list[LogEvent]:
    _import_declaring_modules()
    return loom_members(LogEvent)


def _loom_keys() -> list[LogKey]:
    _import_declaring_modules()
    return loom_members(LogKey)


class TestLoomLogEvents:
    def test_loom_declares_events(self) -> None:
        assert _loom_events()

    def test_every_member_follows_area_subject_fact(self) -> None:
        misnamed = [event for event in _loom_events() if not event.is_well_formed]

        assert misnamed == []

    def test_no_value_names_two_events(self) -> None:
        values = [event.value for event in _loom_events()]

        assert len(values) == len(set(values))


class TestLoomLogKeys:
    def test_loom_declares_keys(self) -> None:
        assert _loom_keys()

    def test_no_value_names_two_keys(self) -> None:
        values = [key.value for key in _loom_keys()]

        assert len(values) == len(set(values))

    def test_an_area_key_starts_with_loom(self) -> None:
        outside = [
            key
            for key in _loom_keys()
            if not isinstance(key, LogField) and not key.startswith("loom.")
        ]

        assert outside == []


class TestSubKey:
    def test_a_member_makes_the_key_of_a_name_inside_its_bag(self) -> None:
        class Sample(LogKey):
            META = "loom.meta"

        assert Sample.META.of("tenant") == "loom.meta.tenant"


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
