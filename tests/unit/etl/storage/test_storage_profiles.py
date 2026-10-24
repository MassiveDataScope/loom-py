"""Tests for ``storage.profiles`` and the YAML-only ``profile:`` key."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import msgspec
import pytest

from loom.core.config import load_config
from loom.core.model.redaction import RedactedMap
from loom.etl.runner.core import ETLRunner
from loom.etl.storage import (
    STORAGE_KEYED_COLLECTIONS,
    StorageConfig,
    StorageProfile,
    normalise_storage_section,
)
from loom.etl.storage._config import convert_storage_config

_LARGE: dict[str, Any] = {
    "storage_options": {"AWS_REGION": "eu-west-1"},
    "writer": {"compression": "ZSTD"},
    "target_file_size": 512,
    "delta_config": {"delta.appendOnly": "true"},
    "commit": {"custom_metadata": {"team": "billing"}},
}


def _large_profile() -> StorageProfile:
    fields = dict(_LARGE)
    fields["storage_options"] = RedactedMap(fields["storage_options"])
    return StorageProfile(**fields)


def _resolve(raw: dict[str, Any]) -> StorageConfig:
    config = convert_storage_config(normalise_storage_section(raw))
    config.validate()
    return config


def _with_profiles(**section: Any) -> dict[str, Any]:
    return {"profiles": {"large": dict(_LARGE)}, **section}


def _storage_section(cfg: Any) -> Any:
    from omegaconf import OmegaConf

    container = OmegaConf.to_container(cfg, resolve=True)
    assert isinstance(container, dict)
    return container["storage"]


# ---------------------------------------------------------------------------
# US3 scenario 1 — the profile fills every field the route did not set
# ---------------------------------------------------------------------------


def test_profile_fills_every_field_the_table_did_not_set() -> None:
    raw = _with_profiles(tables={"orders": {"path": {"uri": "/lake/orders", "profile": "large"}}})

    config = _resolve(raw)

    location = config.tables[0].path
    assert location is not None
    assert location.uri == "/lake/orders"
    assert location.storage_options == _LARGE["storage_options"]
    assert location.writer == _LARGE["writer"]
    assert location.target_file_size == _LARGE["target_file_size"]
    assert location.delta_config == _LARGE["delta_config"]
    assert location.commit == _LARGE["commit"]


def test_profiles_bind_to_storage_config() -> None:
    config = _resolve(_with_profiles())

    assert config.profiles == {"large": _large_profile()}


def test_profile_never_reaches_the_structs() -> None:
    raw = _with_profiles(tables={"orders": {"path": {"uri": "/lake/orders", "profile": "large"}}})

    normalised = normalise_storage_section(raw)

    assert "profile" not in normalised["tables"][0]["path"]


# ---------------------------------------------------------------------------
# US3 scenario 2 — the route's own field wins over the profile, field-level
# ---------------------------------------------------------------------------


def test_route_field_replaces_the_profile_field_entirely() -> None:
    raw = {
        "profiles": {"large": {"writer": {"b": 2}, "target_file_size": 512}},
        "tables": {
            "orders": {"path": {"uri": "/lake/orders", "profile": "large", "writer": {"a": 1}}}
        },
    }

    normalised = normalise_storage_section(raw)

    assert normalised["tables"][0]["path"] == {
        "uri": "/lake/orders",
        "writer": {"a": 1},
        "target_file_size": 512,
    }


# ---------------------------------------------------------------------------
# US3 scenario 3 — unknown profile names the route and the profile
# ---------------------------------------------------------------------------


def test_unknown_profile_on_a_table_names_route_and_profile() -> None:
    raw = _with_profiles(tables={"orders": {"path": {"uri": "/lake/orders", "profile": "missing"}}})

    with pytest.raises(ValueError, match=r"storage\.tables\['orders'\]\.path\.profile='missing'"):
        normalise_storage_section(raw)


def test_unknown_profile_on_a_file_names_route_and_profile() -> None:
    raw = _with_profiles(
        files={"daily": {"path": {"uri": "/out/{date}.csv", "profile": "missing"}}}
    )

    with pytest.raises(ValueError, match=r"storage\.files\['daily'\]\.path\.profile='missing'"):
        normalise_storage_section(raw)


def test_profile_on_a_route_without_profiles_section_is_unknown() -> None:
    raw = {"tables": {"orders": {"path": {"uri": "/lake/orders", "profile": "large"}}}}

    with pytest.raises(ValueError, match="'large' is not defined in storage.profiles"):
        normalise_storage_section(raw)


def test_unknown_profile_on_a_list_form_table_names_route_and_profile() -> None:
    raw = _with_profiles(tables=[{"name": "orders", "path": {"uri": "/x", "profile": "missing"}}])

    with pytest.raises(ValueError, match=r"storage\.tables\['orders'\]\.path\.profile='missing'"):
        normalise_storage_section(raw)


# ---------------------------------------------------------------------------
# US3 scenario 4 — defaults.table_path takes a profile like a table does
# ---------------------------------------------------------------------------


def test_defaults_table_path_takes_the_profile() -> None:
    raw = _with_profiles(defaults={"table_path": {"uri": "/lake", "profile": "large"}})

    config = _resolve(raw)

    location = config.defaults.table_path
    assert location is not None
    assert location.to_location().writer == _LARGE["writer"]
    assert location.to_location().target_file_size == _LARGE["target_file_size"]


def test_unknown_profile_on_defaults_names_the_defaults_path() -> None:
    raw = _with_profiles(defaults={"table_path": {"uri": "/lake", "profile": "missing"}})

    with pytest.raises(ValueError, match=r"storage\.defaults\.table_path\.profile='missing'"):
        normalise_storage_section(raw)


# ---------------------------------------------------------------------------
# US3 scenario 5 — file routes take storage_options only
# ---------------------------------------------------------------------------


def test_file_route_takes_only_storage_options_from_the_profile() -> None:
    raw = _with_profiles(files={"daily": {"path": {"uri": "/out/{date}.csv", "profile": "large"}}})

    config = _resolve(raw)

    assert config.files[0].path.uri == "/out/{date}.csv"
    assert config.files[0].path.storage_options == _LARGE["storage_options"]
    locator = config.to_file_locator()
    assert locator is not None


def test_file_route_ignores_profile_fields_without_a_file_equivalent() -> None:
    raw = _with_profiles(files={"daily": {"path": {"uri": "/out/{date}.csv", "profile": "large"}}})

    normalised = normalise_storage_section(raw)

    assert normalised["files"][0]["path"] == {
        "uri": "/out/{date}.csv",
        "storage_options": _LARGE["storage_options"],
    }


# ---------------------------------------------------------------------------
# US3 scenario 6 — profile declared in one included file, used in another
# ---------------------------------------------------------------------------


def test_profile_declared_in_common_resolves_from_sales(tmp_path: Path) -> None:
    (tmp_path / "common.yaml").write_text(
        "storage:\n  profiles:\n    large:\n      target_file_size: 512\n      writer:\n"
        "        compression: ZSTD\n",
        encoding="utf-8",
    )
    (tmp_path / "sales.yaml").write_text(
        "storage:\n  tables:\n    sales.orders:\n      path:\n"
        f"        uri: {tmp_path}/orders\n        profile: large\n",
        encoding="utf-8",
    )
    storage = tmp_path / "storage.yaml"
    storage.write_text("includes:\n  - common.yaml\n  - sales.yaml\n", encoding="utf-8")

    cfg = load_config(str(storage), keyed=STORAGE_KEYED_COLLECTIONS)
    config = convert_storage_config(normalise_storage_section(_storage_section(cfg)))
    config.validate()

    assert config.tables[0].name == "sales.orders"
    assert config.tables[0].path is not None
    location = config.tables[0].path.to_location()
    assert location.target_file_size == 512
    assert location.writer == {"compression": "ZSTD"}


# ---------------------------------------------------------------------------
# Profile validation — unknown fields and uri
# ---------------------------------------------------------------------------


def test_profile_with_unknown_field_is_rejected_naming_the_field() -> None:
    raw = {
        "profiles": {"large": {"writter": {"a": 1}}},
        "tables": {"orders": {"path": {"uri": "/lake/orders", "profile": "large"}}},
    }

    normalised = normalise_storage_section(raw)

    with pytest.raises(msgspec.ValidationError, match="writter"):
        convert_storage_config(normalised)


def test_profile_with_uri_is_rejected() -> None:
    raw = {
        "profiles": {"large": {"uri": "/lake"}},
        "tables": {"orders": {"path": {"uri": "/lake/orders", "profile": "large"}}},
    }

    with pytest.raises(ValueError, match="uri"):
        normalise_storage_section(raw)


def test_unknown_profile_on_a_nameless_list_route_names_the_index() -> None:
    raw = _with_profiles(tables=[{"path": {"uri": "/x"}}, {"path": {"uri": "/y", "profile": "m"}}])

    with pytest.raises(ValueError, match=r"storage\.tables\[1\]\.path\.profile='m'"):
        normalise_storage_section(raw)


def test_storage_profile_struct_forbids_unknown_fields() -> None:
    with pytest.raises(msgspec.ValidationError, match="writter"):
        msgspec.convert({"writter": {}}, StorageProfile)


def test_route_without_profile_is_left_unchanged() -> None:
    raw = _with_profiles(tables={"orders": {"path": {"uri": "/lake/orders"}}})

    normalised = normalise_storage_section(raw)

    assert normalised["tables"][0]["path"] == {"uri": "/lake/orders"}


def test_non_mapping_path_passes_through_untouched() -> None:
    raw = _with_profiles(tables={"orders": {"path": "not-a-mapping"}}, defaults=3)

    normalised = normalise_storage_section(raw)

    assert normalised["tables"][0]["path"] == "not-a-mapping"
    assert normalised["defaults"] == 3


def test_normalise_does_not_mutate_its_input() -> None:
    tables = {"orders": {"path": {"uri": "/lake/orders", "profile": "large"}}}
    raw = _with_profiles(tables=tables)

    normalise_storage_section(raw)

    assert raw == {"profiles": {"large": _LARGE}, "tables": tables}
    assert "profile" in tables["orders"]["path"]


# ---------------------------------------------------------------------------
# Entry points — strictness stays per entry point
# ---------------------------------------------------------------------------


def _captured_config(build: Any) -> StorageConfig:
    with patch.object(ETLRunner, "_assemble") as assemble:
        build()
    config = assemble.call_args.args[0]
    assert isinstance(config, StorageConfig)
    return config


def test_from_dict_applies_the_profile() -> None:
    raw = _with_profiles(tables={"orders": {"path": {"uri": "/lake/orders", "profile": "large"}}})

    config = _captured_config(lambda: ETLRunner.from_dict(raw))

    assert config.tables[0].path is not None
    assert config.tables[0].path.writer == _LARGE["writer"]
    assert config.profiles == {"large": _large_profile()}


def test_from_yaml_coerces_a_profile_target_file_size_from_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TFS", "512")
    path = tmp_path / "loom.yaml"
    path.write_text(
        "storage:\n  profiles:\n    large:\n      target_file_size: ${oc.env:TFS}\n"
        "  tables:\n    orders:\n      path:\n        uri: /lake/orders\n        profile: large\n",
        encoding="utf-8",
    )

    config = _captured_config(lambda: ETLRunner.from_yaml(str(path)))

    assert config.tables[0].path is not None
    assert config.tables[0].path.target_file_size == 512
    assert config.profiles["large"].target_file_size == 512


def test_from_dict_rejects_a_string_profile_target_file_size() -> None:
    raw = {
        "profiles": {"large": {"target_file_size": "512"}},
        "tables": {"orders": {"path": {"uri": "/lake/orders", "profile": "large"}}},
    }

    with pytest.raises(msgspec.ValidationError):
        ETLRunner.from_dict(raw)
