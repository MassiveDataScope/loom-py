"""Import-containment regression tests for the shared entry-point machinery.

Two invariants are guarded here:

* ``loom.core.plugins.entrypoints`` is a stdlib-only leaf: importing it must not
  pull any other ``loom`` module nor any third-party package into the process.
* The ETL runner stays a leaf consumer of that machinery: importing it may not
  drag the REST pillar (or its ``pydantic``/``fastapi`` dependencies) in.

Both checks run in a clean subprocess because the pytest interpreter has
already imported those modules for other suites.
"""

from __future__ import annotations

from tests.helpers.imports import import_delta

_FORBIDDEN: tuple[str, ...] = ("loom.rest", "pydantic", "fastapi")

_ALLOWED_LOOM_MODULES: frozenset[str] = frozenset(
    {
        "loom",
        "loom.core",
        "loom.core.plugins",
        "loom.core.plugins.entrypoints",
    }
)


class TestPluginImportContainment:
    def test_the_entry_point_loader_imports_nothing_outside_the_stdlib(self) -> None:
        """Importing the entry-point leaf must add no ``loom`` nor third-party module."""
        imported = import_delta("loom.core.plugins.entrypoints")

        unexpected_loom = sorted(imported.loom - _ALLOWED_LOOM_MODULES)
        assert unexpected_loom == [], f"unexpected loom modules imported: {unexpected_loom}"
        assert imported.third_party == frozenset(), (
            f"non-stdlib packages imported: {sorted(imported.third_party)}"
        )

    def test_importing_the_provider_loader_does_not_pull_in_rest_or_pydantic(self) -> None:
        """Importing the ETL provider loader must not pull in REST-only modules."""
        imported = import_delta("loom.etl.runner._engines")

        assert imported.under(*_FORBIDDEN) == frozenset()
