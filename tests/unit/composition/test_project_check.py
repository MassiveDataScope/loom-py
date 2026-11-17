"""``Project.check``: the deploy checks on a configuration loaded as written."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pytest
import yaml

from loom.composition.project import Project
from loom.core.config import ConfigError

_UNAUTHENTICATED = {
    "app": {"name": "acme", "rest": {"docs_url": None, "redoc_url": None, "openapi_url": None}}
}


def _write(path: Path, document: Mapping[str, object]) -> str:
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return str(path)


class TestProjectCheck:
    def test_reports_the_errors_of_a_bad_configuration(self, tmp_path: Path) -> None:
        config = _write(tmp_path / "app.yaml", _UNAUTHENTICATED)

        checked = Project.check(config, environment="test")

        assert checked.report.errors
        assert "app.rest" in checked.report.checked

    def test_leaves_out_the_excluded_path(self, tmp_path: Path) -> None:
        config = _write(tmp_path / "app.yaml", _UNAUTHENTICATED)

        checked = Project.check(config, environment="test", excluded=("app.rest",))

        assert not checked.report.errors
        assert "app.rest (left out by the process)" in [
            skipped.describe() for skipped in checked.report.skipped
        ]

    def test_skips_a_path_the_configuration_does_not_declare(self, tmp_path: Path) -> None:
        config = _write(tmp_path / "worker.yaml", {"app": {"name": "acme"}})

        checked = Project.check(config, environment="test")

        assert "app.rest (not declared)" in [s.describe() for s in checked.report.skipped]
        assert "app.rest" not in checked.report.checked

    def test_a_fastapi_entry_requires_app_rest_though_it_is_not_declared(
        self, tmp_path: Path
    ) -> None:
        config = _write(tmp_path / "api.yaml", {"app": {"name": "acme", "entry": "fastapi"}})

        checked = Project.check(config, environment="test")

        assert "loom.rest.E001" in [result.id for result in checked.report.errors]

    def test_loads_without_resolving_a_placeholder(self, tmp_path: Path) -> None:
        config = _write(tmp_path / "worker.yaml", {"app": {"name": "${secrets:/acme/name}"}})

        checked = Project.check(config, environment="test", excluded=("app.rest",))

        assert checked.config_root == tmp_path.resolve()

    def test_refuses_a_code_path_that_is_not_literal(self, tmp_path: Path) -> None:
        config = _write(
            tmp_path / "app.yaml", {"app": {"name": "acme", "code_path": "${oc.env:CODE_DIR}"}}
        )

        with pytest.raises(ConfigError, match=r"app\.code_path"):
            Project.check(config, environment="test")
