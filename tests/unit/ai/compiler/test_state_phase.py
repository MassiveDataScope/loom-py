"""State phase (T201): ``compile_state`` resolves the one authored shape."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from loom.ai.abc import StateShape
from loom.ai.compiler.phases._state import compile_state
from loom.ai.errors import AgentErrorCode

_SRC = Path(__file__).resolve().parents[4] / "src"
_COMPONENT = "agents/triage.agent.yaml"


@pytest.fixture
def symbol_package_path() -> Iterator[Path]:
    """Make the fake ``myapp`` package importable for symbol references."""
    fixtures = Path(__file__).resolve().parents[1] / "fixtures" / "fake_pkgs"
    path = str(fixtures)
    sys.path.insert(0, path)
    try:
        yield fixtures
    finally:
        sys.path.remove(path)
        for name in [mod for mod in sys.modules if mod.split(".")[0] == "myapp"]:
            del sys.modules[name]


class TestStatePhase:
    def test_declaring_neither_field_produces_no_shape_and_no_issues(self) -> None:
        shape, issues = compile_state(None, None, _COMPONENT)
        assert (shape, issues) == (None, [])

    def test_declaring_both_fields_reports_the_conflict(self) -> None:
        shape, issues = compile_state("dict", {"type": "object"}, _COMPONENT)
        assert shape is None
        assert [issue.code for issue in issues] == [AgentErrorCode.STATE_DECLARATION_CONFLICT]

    def test_dict_waives_the_schema(self) -> None:
        shape, issues = compile_state("dict", None, _COMPONENT)
        assert issues == []
        assert shape == StateShape(schema=None, loom_type=None)

    def test_a_missing_module_reports_unresolvable(self) -> None:
        shape, issues = compile_state("no_such_pkg_zz.domain:Missing", None, _COMPONENT)
        assert shape is None
        assert [issue.code for issue in issues] == [AgentErrorCode.STATE_TYPE_REF_UNRESOLVABLE]

    def test_a_symbol_msgspec_cannot_schematise_reports_unsupported(
        self,
        symbol_package_path: Path,
    ) -> None:
        shape, issues = compile_state("myapp.domain.unsupported:PlainModel", None, _COMPONENT)
        assert shape is None
        assert [issue.code for issue in issues] == [AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED]

    def test_a_resolvable_struct_symbol_compiles_a_boundary_type_with_defaults(
        self,
        symbol_package_path: Path,
    ) -> None:
        shape, issues = compile_state("myapp.domain.triage:TriageReport", None, _COMPONENT)
        assert issues == []
        assert shape is not None
        assert shape.schema is not None
        assert shape.loom_type is not None
        decoded = shape.loom_type.decode_json(b"{}")
        assert shape.loom_type.to_builtins(decoded) == {
            "incident_ref": "",
            "severity": "low",
            "confidence": 0.0,
            "alerts": [],
        }

    def test_a_symbol_admitting_unknown_fields_reports_unsupported(
        self,
        symbol_package_path: Path,
    ) -> None:
        """FR-017: the symbol path forbids unknown fields, matching the output side."""
        shape, issues = compile_state("myapp.domain.unsupported:LaxStruct", None, _COMPONENT)
        assert shape is None
        assert [issue.code for issue in issues] == [AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED]

    def test_a_container_alias_symbol_still_compiles(self, symbol_package_path: Path) -> None:
        """A ``deps_type`` naming a container alias, not a class, keeps compiling (FR-007)."""
        shape, issues = compile_state("myapp.domain.triage:TriageCounts", None, _COMPONENT)
        assert issues == []
        assert shape is not None
        assert shape.schema is not None
        assert shape.loom_type is not None
        decoded = shape.loom_type.decode_json(b'{"critical": 2}')
        assert shape.loom_type.to_builtins(decoded) == {"critical": 2}

    def test_a_strict_pydantic_symbol_compiles(self, symbol_package_path: Path) -> None:
        """SC-003: the schema and the boundary type match a strict pydantic model."""
        shape, issues = compile_state(
            "myapp.domain.pydantic_invoices:InvoiceSummaryModel", None, _COMPONENT
        )
        assert issues == []
        assert shape is not None
        assert shape.loom_type is not None
        assert shape.loom_type.library == "pydantic"
        assert (shape.schema or {}).get("additionalProperties") is False

    def test_a_lax_pydantic_symbol_reports_unsupported(self, symbol_package_path: Path) -> None:
        shape, issues = compile_state("myapp.domain.unsupported:LaxModel", None, _COMPONENT)
        assert shape is None
        assert [issue.code for issue in issues] == [AgentErrorCode.STATE_TYPE_REF_UNSUPPORTED]

    def test_an_invalid_deps_schema_reports_schema_invalid(self) -> None:
        shape, issues = compile_state(None, {"type": 42}, _COMPONENT)
        assert shape is None
        assert [issue.code for issue in issues] == [AgentErrorCode.STATE_SCHEMA_INVALID]

    def test_a_valid_deps_schema_compiles_a_boundary_type(self) -> None:
        schema = {
            "type": "object",
            "properties": {"marca": {"type": "string"}, "km": {"type": "integer"}},
            "required": ["marca"],
        }
        shape, issues = compile_state(None, schema, _COMPONENT)
        assert issues == []
        assert shape is not None
        assert dict(shape.schema or {}) == schema
        assert shape.loom_type is not None
        decoded = shape.loom_type.decode_json(b'{"marca": "civic"}')
        assert shape.loom_type.to_builtins(decoded) == {"marca": "civic", "km": None}


_IMPORT_CHECK_SCRIPT = """
import json
import sys

import loom.ai.compiler.phases._state  # noqa: F401

leaked = [name for name in sys.modules if name == "pydantic_ai" or name.startswith("pydantic_ai.")]
print(json.dumps(sorted(leaked)))
"""


class TestStatePhaseImportContainment:
    def test_the_state_phase_never_imports_pydantic_ai(self) -> None:
        """A clean subprocess: evicting ``sys.modules`` mid-suite would not
        re-execute this module's own dependencies, so it could not see a hidden
        import (the precedent is ``test_import_containment.py``).
        """
        env = {**os.environ, "PYTHONPATH": str(_SRC)}
        result = subprocess.run(
            [sys.executable, "-c", _IMPORT_CHECK_SCRIPT],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
        assert result.returncode == 0, result.stderr

        leaked = json.loads(result.stdout.strip().splitlines()[-1])
        assert leaked == [], f"unexpected pydantic_ai import: {leaked}\n{result.stderr}"
