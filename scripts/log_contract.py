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

    A name's module is not published, so a published name missing from ``lines`` while a
    module failed to import may live in that module: it cannot be checked.

    Attributes:
        lines: One line per declared event and key.
        failed_modules: The modules a missing extra or an unsupported Python kept from importing.
        crashed_modules: The modules that raised anything but ``ImportError`` on import.
    """

    lines: frozenset[str]
    failed_modules: frozenset[str]
    crashed_modules: frozenset[str] = frozenset()

    @classmethod
    def from_loom(cls) -> DeclaredNames:
        """Import every module of ``loom`` and read the names its classes declare.

        Only an ``ImportError`` (a missing extra or an unsupported Python) is an expected
        reason for a module to hide its names; any other exception is a crash.
        """
        failed: set[str] = set()
        crashed: set[str] = set()
        for module in pkgutil.walk_packages(loom.__path__, "loom.", onerror=failed.add):
            try:
                importlib.import_module(module.name)
            except ImportError:
                failed.add(module.name)
            except Exception:
                crashed.add(module.name)
        lines = {
            f"event {event.value}"
            for owner in _subclasses(LogEvent)
            if owner.__module__.startswith("loom.")
            for event in owner
        } | {
            f"key {key.value} {key.kind.value}"
            for owner in _subclasses(LogKey)
            if owner.__module__.startswith("loom.")
            for key in owner
        }
        return cls(frozenset(lines), frozenset(failed), frozenset(crashed))

    def unchecked(self, published: Iterable[str]) -> frozenset[str]:
        """The *published* lines this run cannot verify: the undeclared ones, if a module failed."""
        if not self.failed_modules:
            return frozenset()
        return frozenset(published) - self.lines

    def text(self) -> str:
        """The contract file's content: one sorted line per event and key."""
        return "".join(f"{line}\n" for line in sorted(self.lines))


def main() -> None:
    """Rewrite the contract file, refusing when an import crashed or hides a published line."""
    declared = DeclaredNames.from_loom()
    if declared.crashed_modules:
        raise SystemExit(
            f"modules crashed on import: {', '.join(sorted(declared.crashed_modules))}"
        )
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
