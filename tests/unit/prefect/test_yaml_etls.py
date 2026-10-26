"""Tests for ``loom.prefect.deploy._yaml_etls`` (ETL declarations read from YAML)."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from loom.core.config import ConfigError
from loom.prefect._flow_yaml import read_yaml
from loom.prefect._meta import DEFAULT_STORAGE_CONFIG_PATH
from loom.prefect.deploy._yaml_etls import (
    EtlDeclaration,
    load_declaration,
    read_declarations,
)
from loom.prefect.flow._assemble import load_flow_settings
from tests.fixtures.prefect.pipelines import (
    OrdersChildPipeline,
    OrdersParams,
    OrdersPipeline,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "prefect" / "etls"
PIPELINES = "tests.fixtures.prefect.pipelines"


def _write(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return path


def _single(tmp_path: Path, name: str, **keys: str) -> Path:
    lines = [f"etl: {name}", f"pipeline: {PIPELINES}.OrdersPipeline"]
    lines.extend(f"{key}: {value}" for key, value in keys.items())
    return _write(tmp_path, f"{name}.yaml", "\n".join(lines) + "\n")


def _by_name(declarations: tuple[EtlDeclaration, ...]) -> dict[str, EtlDeclaration]:
    return {declaration.name: declaration for declaration in declarations}


# --- US5 scenario 1: single document ------------------------------------------


def test_single_document_yields_one_declaration_with_etl_flow_settings() -> None:
    path = FIXTURES / "daily_orders.yaml"
    (declaration,) = read_declarations(str(path))
    assert declaration.name == "daily-orders"
    assert declaration.attribute == "daily_orders"
    assert declaration.config_uri == str(path)
    assert declaration.pipeline is OrdersPipeline
    assert declaration.params_type is OrdersParams
    assert declaration.storage_config_path == "/srv/orders/config.yaml"
    assert declaration.settings == load_flow_settings(str(path))


def test_single_document_without_etl_key_uses_file_stem() -> None:
    (declaration,) = read_declarations(str(FIXTURES / "orders_reconcile.yaml"))
    assert declaration.name == "orders_reconcile"
    assert declaration.storage_config_path == DEFAULT_STORAGE_CONFIG_PATH


# --- US5 scenario 2: etls mapping ---------------------------------------------


def test_etls_mapping_yields_one_declaration_per_key() -> None:
    path = FIXTURES / "billing.yaml"
    declarations = _by_name(read_declarations(str(path)))
    assert set(declarations) == {"monthly_close", "invoice_sync"}
    assert declarations["monthly_close"].settings.schedule == {"cron": "0 3 1 * *"}
    assert declarations["monthly_close"].settings.tags == ("billing",)
    assert declarations["invoice_sync"].pipeline is OrdersChildPipeline
    assert all(d.config_uri == str(path) for d in declarations.values())


# --- US5 scenario 3: glob -----------------------------------------------------


def test_glob_processes_every_matching_file() -> None:
    declarations = _by_name(read_declarations(str(FIXTURES / "*.yaml")))
    assert set(declarations) == {
        "daily-orders",
        "monthly_close",
        "invoice_sync",
        "orders_reconcile",
    }


def test_glob_matching_a_file_without_etls_or_pipeline_names_it(tmp_path: Path) -> None:
    _single(tmp_path, "daily_orders")
    stray = _write(tmp_path, "storage.yaml", "storage:\n  tables: {}\n")
    with pytest.raises(ConfigError, match=str(stray)):
        read_declarations(str(tmp_path / "*.yaml"))


# --- US5 scenario 4: dotted paths ---------------------------------------------


@pytest.mark.parametrize(
    "dotted",
    [f"{PIPELINES}.NotAPipeline", f"{PIPELINES}.Missing", "no.such.module.Cls", "bare"],
)
def test_invalid_pipeline_path_names_etl_and_path(tmp_path: Path, dotted: str) -> None:
    path = _write(tmp_path, "orders.yaml", f"etl: orders\npipeline: {dotted}\n")
    with pytest.raises(ConfigError) as excinfo:
        read_declarations(str(path))
    assert "'orders'" in str(excinfo.value)
    assert dotted in str(excinfo.value)


def test_module_raising_at_import_is_a_config_error_naming_etl_and_path(tmp_path: Path) -> None:
    dotted = "tests.fixtures.prefect.raising.Pipeline"
    path = _write(tmp_path, "orders.yaml", f"etl: orders\npipeline: {dotted}\n")
    with pytest.raises(ConfigError) as excinfo:
        read_declarations(str(path))
    assert "'orders'" in str(excinfo.value)
    assert dotted in str(excinfo.value)
    assert "boom at import" in str(excinfo.value)


def test_missing_pipeline_in_etls_entry_names_etl(tmp_path: Path) -> None:
    path = _write(tmp_path, "etls.yaml", "etls:\n  orders:\n    params: {}\n")
    with pytest.raises(ConfigError, match="'orders'.*pipeline"):
        read_declarations(str(path))


def test_params_type_not_a_struct_names_etl_and_path(tmp_path: Path) -> None:
    dotted = f"{PIPELINES}.NotAStruct"
    path = _single(tmp_path, "orders", params_type=dotted)
    with pytest.raises(ConfigError) as excinfo:
        read_declarations(str(path))
    assert "'orders'" in str(excinfo.value)
    assert dotted in str(excinfo.value)


# --- R7: params_type inference ------------------------------------------------


def test_params_type_inferred_from_generic_binding(tmp_path: Path) -> None:
    (declaration,) = read_declarations(str(_single(tmp_path, "orders")))
    assert declaration.params_type is OrdersParams


def test_params_type_inferred_through_inheritance(tmp_path: Path) -> None:
    path = _write(tmp_path, "orders.yaml", f"pipeline: {PIPELINES}.OrdersChildPipeline\n")
    (declaration,) = read_declarations(str(path))
    assert declaration.params_type is OrdersParams


def test_explicit_params_type_wins_over_inference(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "orders.yaml",
        f"pipeline: {PIPELINES}.OrdersPipeline\nparams_type: {PIPELINES}.OtherParams\n",
    )
    (declaration,) = read_declarations(str(path))
    assert declaration.params_type is not OrdersParams
    assert declaration.params_type.__name__ == "OtherParams"


def test_unbound_pipeline_without_params_type_names_etl(tmp_path: Path) -> None:
    path = _write(tmp_path, "orders.yaml", f"pipeline: {PIPELINES}.UnboundPipeline\n")
    with pytest.raises(ConfigError, match="'orders'.*params_type"):
        read_declarations(str(path))


# --- FR-046: names and attributes ---------------------------------------------


@pytest.mark.parametrize("name", ["sales.daily", "2024_close"])
def test_non_identifier_attribute_is_rejected_naming_the_etl(tmp_path: Path, name: str) -> None:
    path = _write(tmp_path, "etl.yaml", f"etl: {name}\npipeline: {PIPELINES}.OrdersPipeline\n")
    with pytest.raises(ConfigError, match=f"'{name}'"):
        read_declarations(str(path))


def test_two_names_collapsing_to_one_attribute_are_rejected(tmp_path: Path) -> None:
    _single(tmp_path, "daily-orders")
    _single(tmp_path, "daily_orders")
    with pytest.raises(ConfigError, match="daily-orders.*daily_orders|daily_orders.*daily-orders"):
        read_declarations(str(tmp_path / "*.yaml"))


def test_same_name_in_two_files_is_rejected(tmp_path: Path) -> None:
    first = _write(tmp_path, "a.yaml", f"etl: orders\npipeline: {PIPELINES}.OrdersPipeline\n")
    second = _write(tmp_path, "b.yaml", f"etl: orders\npipeline: {PIPELINES}.OrdersPipeline\n")
    with pytest.raises(ConfigError) as excinfo:
        read_declarations(str(tmp_path / "*.yaml"))
    assert str(first) in str(excinfo.value)
    assert str(second) in str(excinfo.value)


# --- load_declaration ---------------------------------------------------------


def test_load_declaration_finds_by_attribute() -> None:
    path = str(FIXTURES / "daily_orders.yaml")
    declaration = load_declaration(path, "daily_orders")
    assert declaration.name == "daily-orders"
    assert declaration.config_uri == path


def test_load_declaration_unknown_attribute_lists_known_ones() -> None:
    with pytest.raises(ConfigError, match="invoice_sync.*monthly_close"):
        load_declaration(str(FIXTURES / "billing.yaml"), "nope")


def test_load_declaration_rejects_a_glob() -> None:
    with pytest.raises(ConfigError, match="glob"):
        load_declaration(str(FIXTURES / "*.yaml"), "daily_orders")


def _write_valid_and_broken_siblings(tmp_path: Path) -> Path:
    return _write(
        tmp_path,
        "etls.yaml",
        f"""\
        etls:
          good:
            pipeline: {PIPELINES}.OrdersPipeline
          broken:
            pipeline: no.such.module.Cls
        """,
    )


def test_load_declaration_validates_only_the_requested_attribute(tmp_path: Path) -> None:
    path = _write_valid_and_broken_siblings(tmp_path)
    declaration = load_declaration(str(path), "good")
    assert declaration.pipeline is OrdersPipeline


def test_read_declarations_still_rejects_the_broken_sibling(tmp_path: Path) -> None:
    path = _write_valid_and_broken_siblings(tmp_path)
    with pytest.raises(ConfigError, match="'broken'.*no.such.module.Cls"):
        read_declarations(str(path))


# --- job_variables.env must be a mapping --------------------------------------


def test_job_variables_env_as_a_list_is_rejected_naming_etl_and_environment(
    tmp_path: Path,
) -> None:
    path = _write(
        tmp_path,
        "orders.yaml",
        f"""\
        etl: orders
        pipeline: {PIPELINES}.OrdersPipeline
        environments:
          production:
            job_variables:
              env: [FOO=bar]
        """,
    )
    with pytest.raises(ConfigError, match="'orders'.*prod.*job_variables.env"):
        read_declarations(str(path))


def test_job_variables_as_a_list_is_rejected_naming_etl_and_environment(
    tmp_path: Path,
) -> None:
    path = _write(
        tmp_path,
        "orders.yaml",
        f"""\
        etl: orders
        pipeline: {PIPELINES}.OrdersPipeline
        environments:
          production:
            job_variables: [cpu, 512]
        """,
    )
    with pytest.raises(ConfigError, match="'orders'.*prod.*job_variables must be a mapping"):
        read_declarations(str(path))


# --- read_yaml keyed etls -----------------------------------------------------


def test_read_yaml_rejects_duplicate_etl_key_across_includes(tmp_path: Path) -> None:
    first = _write(tmp_path, "a.yaml", "etls:\n  orders: {pipeline: x}\n")
    second = _write(tmp_path, "b.yaml", "etls:\n  orders: {pipeline: y}\n")
    root = _write(tmp_path, "root.yaml", "includes: [a.yaml, b.yaml]\n")
    with pytest.raises(ConfigError) as excinfo:
        read_yaml(str(root))
    assert str(first) in str(excinfo.value)
    assert str(second) in str(excinfo.value)


def test_trigger_inheriting_an_undeclared_parameter_is_a_config_error(tmp_path: Path) -> None:
    path = _single(tmp_path, "orders", trigger="{after: upstream, inherit_params: [run_day]}")
    with pytest.raises(ConfigError, match=rf"{path}.*'run_day'.*OrdersParams"):
        read_declarations(str(path))


def test_trigger_inheriting_a_declared_parameter_is_accepted(tmp_path: Path) -> None:
    path = _single(tmp_path, "orders", trigger="{after: upstream, inherit_params: [run_date]}")
    (declaration,) = read_declarations(str(path))
    assert declaration.settings.trigger is not None
    assert declaration.settings.trigger.inherit_params == ("run_date",)


def test_an_invalid_trigger_block_is_a_config_error(tmp_path: Path) -> None:
    path = _single(tmp_path, "orders", trigger="{after: upstream, on: Failed}")
    with pytest.raises(ConfigError, match=rf"{path}: ETL 'orders': trigger:.*unknown field"):
        read_declarations(str(path))
