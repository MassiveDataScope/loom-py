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


@pytest.fixture(scope="module")
def declared_names() -> None:
    for path in source_files():
        if any(marker in path.read_text() for marker in _DECLARATIONS):
            module = ".".join(path.relative_to(_SRC).with_suffix("").parts)
            try:
                importlib.import_module(module)
            except ModuleNotFoundError as missing:
                pytest.skip(f"{module} needs {missing.name}")


@pytest.mark.usefixtures("declared_names")
class TestLoomLogNames:
    @pytest.mark.parametrize("root", [LogEvent, LogKey])
    def test_loom_declares_members(self, root: type[LogEvent | LogKey]) -> None:
        assert loom_members(root)

    @pytest.mark.parametrize("root", [LogEvent, LogKey])
    def test_no_value_names_two_members(self, root: type[LogEvent | LogKey]) -> None:
        values = [member.value for member in loom_members(root)]

        assert len(values) == len(set(values))

    def test_every_event_follows_area_subject_fact(self) -> None:
        misnamed = [event for event in loom_members(LogEvent) if not event.is_well_formed]

        assert misnamed == []

    def test_an_area_key_starts_with_loom(self) -> None:
        outside = [
            key
            for key in loom_members(LogKey)
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
        ("value", "well_formed"),
        [
            ("job.delivery_abandoned", True),
            ("observability.lifecycle_started", True),
            ("a.b_c_d", True),
            ("JobAbandoned", False),
            ("job_abandoned", False),
            ("job.abandoned", False),
            ("Job.delivery_abandoned", False),
            ("[BOOT] x", False),
        ],
    )
    def test_only_a_name_in_the_form_is_well_formed(self, value: str, well_formed: bool) -> None:
        class Sample(LogEvent):
            EVENT = value

        assert Sample.EVENT.is_well_formed is well_formed
