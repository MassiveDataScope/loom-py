"""Tests for the mapping form of ``storage.tables`` and ``storage.files``."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import msgspec
import pytest

from loom.etl.runner.core import ETLRunner
from loom.etl.storage import (
    FileRoute,
    StorageConfig,
    TableRoute,
    normalise_storage_section,
)
from loom.etl.storage._config import convert_storage_config

_LIST_FORM: dict[str, Any] = {
    "defaults": {"table_path": {"uri": "/lake"}},
    "tables": [
        {"name": "sales.orders", "path": {"uri": "/lake/orders", "target_file_size": 1024}},
        {"name": "billing.invoices", "path": {"uri": "/lake/invoices"}},
    ],
    "files": [{"name": "exports.daily", "path": {"uri": "/out/{date}.csv"}}],
}

_MAPPING_FORM: dict[str, Any] = {
    "defaults": {"table_path": {"uri": "/lake"}},
    "tables": {
        "sales.orders": {"path": {"uri": "/lake/orders", "target_file_size": 1024}},
        "billing.invoices": {"path": {"uri": "/lake/invoices"}},
    },
    "files": {"exports.daily": {"path": {"uri": "/out/{date}.csv"}}},
}

_LIST_YAML = """
storage:
  defaults:
    table_path:
      uri: {root}
  tables:
    - name: sales.orders
      path:
        uri: {root}/orders
        target_file_size: 1024
    - name: billing.invoices
      path:
        uri: {root}/invoices
  files:
    - name: exports.daily
      path:
        uri: {root}/out/{{date}}.csv
"""

_MAPPING_YAML = """
storage:
  defaults:
    table_path:
      uri: {root}
  tables:
    sales.orders:
      path:
        uri: {root}/orders
        target_file_size: 1024
    billing.invoices:
      path:
        uri: {root}/invoices
  files:
    exports.daily:
      path:
        uri: {root}/out/{{date}}.csv
"""


def _captured_config(build: Any) -> StorageConfig:
    with patch.object(ETLRunner, "_assemble") as assemble:
        build()
    config = assemble.call_args.args[0]
    assert isinstance(config, StorageConfig)
    return config


def _write(tmp_path: Path, name: str, template: str) -> str:
    target = tmp_path / name
    target.write_text(template.format(root=str(tmp_path)), encoding="utf-8")
    return str(target)


# ---------------------------------------------------------------------------
# US2 scenario 1 — mapping form binds to the same routes as the list form
# ---------------------------------------------------------------------------


def test_mapping_form_converts_to_the_same_routes_as_list_form() -> None:
    from_mapping = convert_storage_config(normalise_storage_section(_MAPPING_FORM))
    from_list = convert_storage_config(normalise_storage_section(_LIST_FORM))

    assert from_mapping == from_list
    assert from_mapping.tables == (
        TableRoute(name="sales.orders", path=from_list.tables[0].path),
        TableRoute(name="billing.invoices", path=from_list.tables[1].path),
    )
    assert from_mapping.files == (FileRoute(name="exports.daily", path=from_list.files[0].path),)


def test_list_form_is_left_unchanged() -> None:
    assert normalise_storage_section(_LIST_FORM) == _LIST_FORM


def test_normalise_does_not_mutate_its_input() -> None:
    raw = {"tables": {"sales.orders": {"ref": "main.sales.orders"}}}

    normalise_storage_section(raw)

    assert raw == {"tables": {"sales.orders": {"ref": "main.sales.orders"}}}


def test_from_yaml_mapping_form_equals_list_form(tmp_path: Path) -> None:
    list_path = _write(tmp_path, "list.yaml", _LIST_YAML)
    mapping_path = _write(tmp_path, "mapping.yaml", _MAPPING_YAML)

    from_list = _captured_config(lambda: ETLRunner.from_yaml(list_path))
    from_mapping = _captured_config(lambda: ETLRunner.from_yaml(mapping_path))

    assert from_mapping == from_list
    assert [route.name for route in from_mapping.tables] == ["sales.orders", "billing.invoices"]
    assert from_mapping.tables[0].path is not None
    assert from_mapping.tables[0].path.target_file_size == 1024


def test_from_dict_mapping_form_equals_list_form() -> None:
    from_list = _captured_config(lambda: ETLRunner.from_dict(_LIST_FORM))
    from_mapping = _captured_config(lambda: ETLRunner.from_dict(_MAPPING_FORM))

    assert from_mapping == from_list


# ---------------------------------------------------------------------------
# US2 scenario 6 — list form with a duplicate name fails as today
# ---------------------------------------------------------------------------


def test_list_form_with_duplicate_name_still_fails() -> None:
    raw = {
        "defaults": {"table_path": {"uri": "/lake"}},
        "tables": [{"name": "dup", "ref": "a.b"}, {"name": "dup", "ref": "c.d"}],
    }

    config = convert_storage_config(normalise_storage_section(raw))

    with pytest.raises(ValueError, match="storage.tables contains duplicate name 'dup'"):
        config.validate()


# ---------------------------------------------------------------------------
# Inner ``name`` handling
# ---------------------------------------------------------------------------


def test_conflicting_inner_name_is_rejected() -> None:
    raw = {"tables": {"sales.orders": {"name": "other", "ref": "a.b"}}}

    with pytest.raises(ValueError, match=r"storage\.tables\['sales\.orders'\].*'other'"):
        normalise_storage_section(raw)


def test_conflicting_inner_name_in_files_is_rejected() -> None:
    raw = {"files": {"exports": {"name": "other", "path": {"uri": "/x"}}}}

    with pytest.raises(ValueError, match=r"storage\.files\['exports'\].*'other'"):
        normalise_storage_section(raw)


def test_inner_name_equal_to_key_is_accepted() -> None:
    raw = {"tables": {"sales.orders": {"name": "sales.orders", "ref": "a.b"}}}

    assert normalise_storage_section(raw) == {
        "tables": [{"name": "sales.orders", "ref": "a.b"}],
    }


# ---------------------------------------------------------------------------
# Non-mapping shapes pass through so conversion reports them
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        {"tables": "x"},
        {"files": "x"},
        {"defaults": 3},
        {"tables": [{"name": "t", "path": {"uri": 123}}]},
        {"tables": {"t": {"path": {"uri": 123}}}},
        {"tables": {"t": 3}},
    ],
)
def test_non_mapping_shapes_pass_through_to_conversion(raw: dict[str, Any]) -> None:
    normalised = normalise_storage_section(raw)

    with pytest.raises(msgspec.ValidationError):
        convert_storage_config(normalised)


def test_from_dict_invalid_shape_raises_validation_error() -> None:
    with pytest.raises(msgspec.ValidationError):
        ETLRunner.from_dict({"tables": {"t": {"path": {"uri": 123}}}})
