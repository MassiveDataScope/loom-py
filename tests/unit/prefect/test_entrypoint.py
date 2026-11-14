"""Tests for ``loom.prefect.deploy.entrypoint`` (the module Prefect imports)."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from prefect.flows import load_flow_from_entrypoint

from loom.core.config import ConfigError
from loom.prefect._meta import LOOM_ETL_META_ATTR, ETLFlowMeta
from loom.prefect.deploy import entrypoint

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "prefect" / "etls"
ENV_VAR = "LOOM_ETL_CONFIG"


def test_public_attribute_without_variable_raises_config_error_naming_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ENV_VAR, raising=False)
    with pytest.raises(ConfigError, match=ENV_VAR):
        load_flow_from_entrypoint("loom.prefect.deploy.entrypoint.daily_orders")


def test_private_probe_without_variable_behaves_like_a_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ENV_VAR, raising=False)
    assert hasattr(entrypoint, "__path__") is False
    with pytest.raises(AttributeError):
        entrypoint._private  # noqa: B018 - attribute access is the behaviour under test


def test_attribute_rebuilds_flow_from_declaration(monkeypatch: pytest.MonkeyPatch) -> None:
    config_uri = str(FIXTURES / "daily_orders.yaml")
    monkeypatch.setenv(ENV_VAR, config_uri)
    flow_obj = load_flow_from_entrypoint("loom.prefect.deploy.entrypoint.daily_orders")
    meta: ETLFlowMeta = getattr(flow_obj, LOOM_ETL_META_ATTR)
    assert flow_obj.name == "daily-orders"
    assert meta.name == "daily-orders"
    assert meta.config_path == config_uri
    assert meta.source_file == entrypoint.__file__
    assert meta.correlation_field == "run_date"
    assert meta.tags == ("orders", "daily")
    assert flow_obj.retries == 3
    assert flow_obj.retry_delay_seconds == 30


def test_unknown_attribute_lists_known_ones(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_VAR, str(FIXTURES / "billing.yaml"))
    with pytest.raises(ConfigError, match="invoice_sync.*monthly_close"):
        entrypoint.nope  # noqa: B018 - attribute access is the behaviour under test


def test_attribute_loads_despite_a_broken_sibling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "etls.yaml"
    path.write_text(
        textwrap.dedent(
            """\
            etls:
              good:
                pipeline: tests.fixtures.prefect.pipelines.OrdersPipeline
              broken:
                pipeline: no.such.module.Cls
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(ENV_VAR, str(path))
    flow_obj = load_flow_from_entrypoint("loom.prefect.deploy.entrypoint.good")
    assert flow_obj.name == "good"


def test_flow_package_never_imports_the_deploy_package() -> None:
    """Import ``loom.prefect.flow`` under a stub parent so ``loom.prefect.__init__``
    (which re-exports the deployer) does not mask a flow → deploy edge."""
    probe = textwrap.dedent(
        """\
        import sys, types
        import loom.prefect as real
        parent = types.ModuleType("loom.prefect")
        parent.__path__ = list(real.__path__)
        for name in [m for m in sys.modules if m.startswith("loom.prefect")]:
            del sys.modules[name]
        sys.modules["loom.prefect"] = parent
        import loom.prefect.flow
        print([m for m in sys.modules if m.startswith("loom.prefect.deploy")])
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[]"


_FIRST_IMPORTS = (
    "loom.prefect.flow",
    "loom.prefect.deploy",
    "loom.prefect.deploy.entrypoint",
    "loom.prefect.flow._assemble",
)

_IMPORT_EACH_FIRST = textwrap.dedent(
    """\
    import importlib, sys, traceback
    failed = {}
    for module in sys.argv[1:]:
        for name in [m for m in sys.modules if m == "loom" or m.startswith("loom.")]:
            del sys.modules[name]
        try:
            importlib.import_module(module)
        except BaseException:
            failed[module] = traceback.format_exc()
    print(failed or "")
    """
)


def test_each_module_imports_first_with_no_loom_module_loaded() -> None:
    """One interpreter: loom is unloaded before each import, so each one runs first."""
    result = subprocess.run(  # noqa: S603 - fixed probe, fixed module names
        [sys.executable, "-c", _IMPORT_EACH_FIRST, *_FIRST_IMPORTS],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "", result.stdout
