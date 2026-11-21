"""Every log event loom declares lives in a ``log_events.py``, under its package's prefix."""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest

from loom.core.logger import LogEvent
from tests.helpers.source_tree import SRC, parsed, source_files

_SRC_ROOT = SRC.parent
_EVENTS_MODULE = "log_events.py"


def _declares_log_event(path: Path) -> bool:
    return any(
        isinstance(node, ast.ClassDef)
        and any(isinstance(base, ast.Name) and base.id == "LogEvent" for base in node.bases)
        for node in ast.walk(parsed(path))
    )


def _modules_declaring_events() -> list[Path]:
    return [path for path in source_files() if _declares_log_event(path)]


def _loom_events() -> list[LogEvent]:
    for path in _modules_declaring_events():
        module = ".".join(path.relative_to(_SRC_ROOT).with_suffix("").parts)
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

    def test_every_event_class_lives_in_a_log_events_module(self) -> None:
        misplaced = [
            str(path.relative_to(_SRC_ROOT))
            for path in _modules_declaring_events()
            if path.name != _EVENTS_MODULE
        ]

        assert misplaced == []

    def test_every_member_starts_with_its_class_prefix_and_is_dotted_lowercase(self) -> None:
        misnamed = [event for event in _loom_events() if not event.is_well_formed]

        assert misnamed == []

    def test_no_value_names_two_events(self) -> None:
        values = [event.value for event in _loom_events()]

        assert len(values) == len(set(values))


class TestEventPrefix:
    def test_a_member_of_the_class_starts_with_its_prefix(self) -> None:
        class Sample(LogEvent, prefix="billing"):
            ISSUED = "billing.invoice_issued"

        assert Sample.prefix == "billing"
        assert Sample.ISSUED.is_well_formed

    def test_a_member_outside_the_prefix_is_not_well_formed(self) -> None:
        class Sample(LogEvent, prefix="billing"):
            ISSUED = "invoicing.invoice_issued"

        assert not Sample.ISSUED.is_well_formed

    def test_a_class_without_a_prefix_cannot_be_declared(self) -> None:
        with pytest.raises(TypeError, match="prefix"):

            class Sample(LogEvent):  # type: ignore[call-arg]
                ISSUED = "billing.invoice_issued"

    @pytest.mark.parametrize("prefix", ["Billing", "billing.", "", "2fa", "a b"])
    def test_a_prefix_that_is_not_lowercase_words_is_refused(self, prefix: str) -> None:
        with pytest.raises(ValueError, match="prefix"):

            class Sample(LogEvent, prefix=prefix):
                ISSUED = "billing.invoice_issued"


class TestEventForm:
    @pytest.mark.parametrize(
        "value",
        ["job.delivery_abandoned", "use_case.execution_succeeded", "etl.checkpoint.read_hit"],
    )
    def test_dotted_lowercase_words_are_well_formed(self, value: str) -> None:
        class Sample(LogEvent, prefix=value.split(".")[0]):
            EVENT = value

        assert Sample.EVENT.is_well_formed

    @pytest.mark.parametrize(
        "value", ["JobAbandoned", "job_abandoned", "Job.delivery_abandoned", "[BOOT] x", "job."]
    )
    def test_anything_else_is_not(self, value: str) -> None:
        class Sample(LogEvent, prefix="job"):
            EVENT = value

        assert not Sample.EVENT.is_well_formed
