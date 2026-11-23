"""``loom.core.log_names`` is a leaf: naming what is logged costs no other import."""

from __future__ import annotations

from tests.helpers.imports import import_delta


class TestLogNamesIsALeaf:
    def test_importing_it_loads_no_other_loom_module_and_nothing_third_party(self) -> None:
        imported = import_delta("loom.core.log_names")

        assert imported.loom <= {"loom", "loom.core", "loom.core.log_names"}
        assert imported.third_party == frozenset()
