"""``Project.check``: the deploy checks on a configuration loaded as written."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pytest
import yaml

from loom.composition.project import Project
from loom.core.config import ConfigError

_UNAUTHENTICATED = {"app": {"rest": {"docs_url": None, "redoc_url": None, "openapi_url": None}}}


def _write(path: Path, document: Mapping[str, object]) -> str:
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return str(path)


class TestProjectCheck:
    def test_reports_the_errors_of_a_bad_configuration(self, tmp_path: Path) -> None:
        config = _write(tmp_path / "app.yaml", _UNAUTHENTICATED)

        checked = Project.check(config, environment="test")

        assert checked.report.errors
        assert checked.skipped == ()

    def test_leaves_out_the_skipped_area(self, tmp_path: Path) -> None:
        config = _write(tmp_path / "app.yaml", _UNAUTHENTICATED)

        checked = Project.check(config, environment="test", skip=("loom.rest",))

        assert not checked.report.errors
        assert checked.skipped == ("loom.rest",)

    def test_loads_without_an_application_name_or_a_resolved_placeholder(
        self, tmp_path: Path
    ) -> None:
        config = _write(tmp_path / "worker.yaml", {"app": {"key": "${secrets:/acme/key}"}})

        checked = Project.check(config, environment="test", skip=("loom.rest",))

        assert checked.config_root == tmp_path.resolve()

    def test_refuses_a_code_path_that_is_not_literal(self, tmp_path: Path) -> None:
        config = _write(tmp_path / "app.yaml", {"app": {"code_path": "${oc.env:CODE_DIR}"}})

        with pytest.raises(ConfigError, match=r"app\.code_path"):
            Project.check(config, environment="test")
