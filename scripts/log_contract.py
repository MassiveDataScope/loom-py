"""Publish loom's log names, the contract that after 12.0 may only grow.

``python -m scripts.log_contract`` rewrites ``docs/reference/log-contract.txt`` from
the :class:`LogEvent` and :class:`LogKey` classes the ``loom`` package declares.
"""

from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TypeVar

import loom
from loom.core.log_names import LogEvent, LogKey

_Names = TypeVar("_Names", bound=Enum)

CONTRACT_FILE = Path(__file__).resolve().parents[1] / "docs" / "reference" / "log-contract.txt"


def _subclasses(base: type[_Names]) -> set[type[_Names]]:
    found: set[type[_Names]] = set()
    for subclass in base.__subclasses__():
        found |= {subclass} | _subclasses(subclass)
    return found


@dataclass(frozen=True, slots=True)
class DeclaredNames:
    """The log names of the loom modules that could be imported, and the modules that could not.

    Each line ends with the module that declares it, so a line is ``covered_by_failure``
    when its module, or a package above it, failed to import.
    """

    lines: frozenset[str]
    failed_modules: frozenset[str]

    @classmethod
    def from_loom(cls) -> DeclaredNames:
        """Import every module of ``loom`` and read the names its classes declare."""
        failed: set[str] = set()
        for module in pkgutil.walk_packages(loom.__path__, "loom.", onerror=failed.add):
            try:
                importlib.import_module(module.name)
            except Exception:
                failed.add(module.name)
        lines = {
            f"event {event.value} {owner.__module__}"
            for owner in _subclasses(LogEvent)
            if owner.__module__.startswith("loom.")
            for event in owner
        } | {
            f"key {key.value} {key.kind.value} {owner.__module__}"
            for owner in _subclasses(LogKey)
            if owner.__module__.startswith("loom.")
            for key in owner
        }
        return cls(frozenset(lines), frozenset(failed))

    def covered_by_failure(self, line: str) -> bool:
        """Whether *line* belongs to a module that failed to import, or to one beneath it."""
        module = line.rsplit(" ", 1)[1]
        return any(module == name or module.startswith(f"{name}.") for name in self.failed_modules)

    def unchecked(self, published: Iterable[str]) -> frozenset[str]:
        """The *published* lines this run could not verify because their module failed."""
        return frozenset(line for line in published if self.covered_by_failure(line))

    def text(self) -> str:
        """The contract file's content: one sorted line per event and key."""
        return "".join(f"{line}\n" for line in sorted(self.lines))


def main() -> None:
    """Rewrite the contract file, refusing when a failed import hides a published line."""
    declared = DeclaredNames.from_loom()
    published = CONTRACT_FILE.read_text().splitlines() if CONTRACT_FILE.exists() else None
    if published is None or declared.unchecked(published):
        raise SystemExit(
            "run on Python < 3.13 with every extra (`uv sync --all-extras --all-groups`): "
            f"{CONTRACT_FILE} is unreadable or lines of the modules that failed to import "
            f"({', '.join(sorted(declared.failed_modules)) or 'none'}) would be lost"
        )
    CONTRACT_FILE.write_text(declared.text())


if __name__ == "__main__":
    main()
