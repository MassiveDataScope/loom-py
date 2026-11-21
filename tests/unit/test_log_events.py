"""The log vocabulary of loom: where it is declared, its form, and its snapshotted contract.

Every ``LogEvent`` and ``LogKey`` subclass loom declares is read by importing
the module that declares it, so a module that cannot import fails here instead
of dropping out of the checks.
"""

from __future__ import annotations

import ast
import importlib
from enum import StrEnum
from functools import cache
from pathlib import Path
from typing import TypeVar

import pytest

from loom.core.logger import LogEvent, LogKey
from tests.helpers.source_tree import SRC, parsed, source_files

_Member = TypeVar("_Member", bound=StrEnum)

_SRC_ROOT = SRC.parent
_CONTRACT = _SRC_ROOT.parent / "docs" / "reference" / "log-contract.txt"
_VOCABULARY_MODULES = frozenset({"log_events.py", "vocabulary.py"})
_BASES = frozenset({"LogEvent", "LogKey", "LogField"})
_VALUE_CARRYING_KEYS = frozenset(
    {
        "url.full",
        "url.query",
        "db.query.text",
        "client.address",
        "user_agent.original",
        "messaging.message.body",
        "gen_ai.input.messages",
        "gen_ai.output.messages",
        "gen_ai.system_instructions",
        "gen_ai.prompt",
        "gen_ai.completion",
    }
)


def _declares_vocabulary(path: Path) -> bool:
    return any(
        isinstance(node, ast.ClassDef)
        and any(isinstance(base, ast.Name) and base.id in _BASES for base in node.bases)
        for node in ast.walk(parsed(path))
    )


@cache
def _vocabulary_modules() -> tuple[Path, ...]:
    return tuple(path for path in source_files() if _declares_vocabulary(path))


@cache
def _unimportable() -> dict[str, str]:
    """The missing module of each vocabulary module that does not import."""
    missing: dict[str, str] = {}
    for path in _vocabulary_modules():
        module = ".".join(path.relative_to(_SRC_ROOT).with_suffix("").parts)
        try:
            importlib.import_module(module)
        except ModuleNotFoundError as error:
            missing[module] = str(error.name)
    return missing


def _members_of(root: type[_Member]) -> list[_Member]:
    _unimportable()
    pending, found = [root], []
    while pending:
        base = pending.pop()
        pending.extend(base.__subclasses__())
        if base.__module__.startswith("loom."):
            found.extend(base)
    return found


def contract() -> list[str]:
    """The sorted names of every loom event and field key: the snapshotted contract."""
    return sorted({str(member) for root in (LogEvent, LogKey) for member in _members_of(root)})


class TestLoomVocabulary:
    def test_every_module_declaring_events_or_keys_imports(self) -> None:
        """The checks below read what imported, so a module that cannot must fail, not vanish.

        The unit lane runs with every extra installed: nothing is expected to be missing.
        """
        assert _unimportable() == {}

    def test_every_event_and_key_class_lives_in_a_log_events_module(self) -> None:
        misplaced = [
            str(path.relative_to(_SRC_ROOT))
            for path in _vocabulary_modules()
            if path.name not in _VOCABULARY_MODULES
        ]

        assert misplaced == []

    def test_every_event_is_dotted_lowercase_words(self) -> None:
        assert [event for event in _members_of(LogEvent) if not event.is_well_formed] == []

    @pytest.mark.parametrize("root", [LogEvent, LogKey], ids=["events", "keys"])
    def test_no_value_is_declared_twice(self, root: type[StrEnum]) -> None:
        values = [member.value for member in _members_of(root)]

        assert sorted({v for v in values if values.count(v) > 1}) == []

    def test_no_key_carries_a_value_of_the_business_or_of_a_person(self) -> None:
        assert sorted(_VALUE_CARRYING_KEYS & {key.value for key in _members_of(LogKey)}) == []

    def test_the_names_equal_the_snapshotted_contract(self) -> None:
        """Before 12.0 the file is regenerated freely; after it, it may only grow."""
        assert contract() == _CONTRACT.read_text(encoding="utf-8").split()


class TestEventForm:
    @pytest.mark.parametrize(
        "value", ["job.delivery_abandoned", "observability.lifecycle_started", "a.b_c_d"]
    )
    def test_a_name_in_the_form_is_well_formed(self, value: str) -> None:
        class Sample(LogEvent):
            EVENT = value

        assert Sample.EVENT.is_well_formed

    @pytest.mark.parametrize(
        "value", ["JobAbandoned", "job_abandoned", "Job.delivery_abandoned", "job.", "[BOOT] x"]
    )
    def test_a_name_outside_the_form_is_not(self, value: str) -> None:
        class Sample(LogEvent):
            EVENT = value

        assert not Sample.EVENT.is_well_formed
