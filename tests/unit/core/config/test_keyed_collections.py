"""Tests for keyed collections merged by key across ``includes`` and explicit layers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigError, load_config

KEYED = ("things.items",)

_MAPPING_A = "things:\n  items:\n    alpha: {size: 1}\n"
_MAPPING_B = "things:\n  items:\n    beta: {size: 2}\n"
_MAPPING_A_DUP = "things:\n  items:\n    alpha: {size: 9}\n"
_LIST_A = "things:\n  items:\n    - {name: alpha}\n"
_LIST_B = "things:\n  items:\n    - {name: beta}\n"


def _write_tree(root: Path, files: dict[str, str]) -> None:
    for name, content in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)


def _as_dict(cfg: Any) -> Any:
    from omegaconf import OmegaConf

    return OmegaConf.to_container(cfg, resolve=True)


def _includes(*entries: str) -> str:
    return "includes:\n" + "".join(f"  - {entry}\n" for entry in entries)


# ---------------------------------------------------------------------------
# US2 scenario 2 — mappings from two includes merge by key
# ---------------------------------------------------------------------------


class TestKeyedMappingsMerge:
    def test_mappings_from_two_includes_merge_into_union(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "a.yaml": _MAPPING_A,
                "b.yaml": _MAPPING_B,
                "root.yaml": _includes("a.yaml", "b.yaml"),
            },
        )

        cfg = load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

        assert _as_dict(cfg) == {"things": {"items": {"alpha": {"size": 1}, "beta": {"size": 2}}}}

    def test_keyed_path_absent_from_every_layer_is_accepted(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, {"a.yaml": "x: 1\n", "root.yaml": _includes("a.yaml") + "y: 2\n"})

        cfg = load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

        assert _as_dict(cfg) == {"x": 1, "y": 2}


class TestKeyedDuplicateKeys:
    def test_duplicate_key_across_two_includes_names_key_and_both_files(
        self, tmp_path: Path
    ) -> None:
        _write_tree(
            tmp_path,
            {
                "a.yaml": _MAPPING_A,
                "c.yaml": _MAPPING_A_DUP,
                "root.yaml": _includes("a.yaml", "c.yaml"),
            },
        )

        with pytest.raises(ConfigError, match=r"things\.items\['alpha'\]") as exc_info:
            load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

        message = str(exc_info.value)
        assert "a.yaml" in message
        assert "c.yaml" in message

    def test_duplicate_through_nested_include_names_declaring_files(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "a.yaml": _MAPPING_A,
                "b.yaml": _includes("c.yaml"),
                "c.yaml": _MAPPING_A_DUP,
                "root.yaml": _includes("a.yaml", "b.yaml"),
            },
        )

        with pytest.raises(ConfigError) as exc_info:
            load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

        message = str(exc_info.value)
        assert "a.yaml" in message
        assert "c.yaml" in message
        assert "b.yaml" not in message.split("(included from")[0]

    def test_duplicate_between_included_file_and_declaring_file_names_both(
        self, tmp_path: Path
    ) -> None:
        _write_tree(
            tmp_path,
            {"a.yaml": _MAPPING_A, "root.yaml": _includes("a.yaml") + _MAPPING_A_DUP},
        )

        with pytest.raises(ConfigError, match=r"things\.items\['alpha'\]") as exc_info:
            load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

        message = str(exc_info.value)
        assert "a.yaml" in message
        assert "root.yaml" in message

    def test_from_yaml_forwards_keyed_to_the_loader(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "a.yaml": _MAPPING_A,
                "c.yaml": _MAPPING_A_DUP,
                "root.yaml": _includes("a.yaml", "c.yaml"),
            },
        )

        with pytest.raises(ConfigError, match=r"things\.items\['alpha'\]"):
            ConfigContext.from_yaml(str(tmp_path / "root.yaml"), keyed=KEYED)


class TestKeyedExplicitLayers:
    def test_duplicate_across_explicit_layers_later_wins(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, {"base.yaml": _MAPPING_A, "prod.yaml": _MAPPING_A_DUP})

        cfg = load_config(str(tmp_path / "base.yaml"), str(tmp_path / "prod.yaml"), keyed=KEYED)

        assert _as_dict(cfg) == {"things": {"items": {"alpha": {"size": 9}}}}


class TestKeyedListsVersusMappings:
    def test_list_in_every_layer_merges_as_today(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {"a.yaml": _LIST_A, "b.yaml": _LIST_B, "root.yaml": _includes("a.yaml", "b.yaml")},
        )

        cfg = load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

        assert _as_dict(cfg) == {"things": {"items": [{"name": "beta"}]}}

    def test_list_and_mapping_across_includes_is_an_error(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {"a.yaml": _LIST_A, "b.yaml": _MAPPING_B, "root.yaml": _includes("a.yaml", "b.yaml")},
        )

        with pytest.raises(ConfigError, match="use one form within a composition") as exc_info:
            load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

        message = str(exc_info.value)
        assert "things.items is a list in" in message
        assert "a.yaml" in message
        assert "b.yaml" in message

    def test_list_and_mapping_across_explicit_layers_is_an_error(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, {"base.yaml": _LIST_A, "prod.yaml": _MAPPING_B})

        with pytest.raises(ConfigError, match="use one form within a composition") as exc_info:
            load_config(str(tmp_path / "base.yaml"), str(tmp_path / "prod.yaml"), keyed=KEYED)

        message = str(exc_info.value)
        assert "base.yaml" in message
        assert "prod.yaml" in message


class TestUnkeyedIncludesOverride:
    def test_without_keyed_a_duplicate_across_includes_is_overridden(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "a.yaml": _MAPPING_A,
                "c.yaml": _MAPPING_A_DUP,
                "root.yaml": _includes("a.yaml", "c.yaml"),
            },
        )

        cfg = load_config(str(tmp_path / "root.yaml"))

        assert _as_dict(cfg) == {"things": {"items": {"alpha": {"size": 9}}}}


class TestKeyedPathsThatCannotBeSelected:
    def test_keyed_path_under_a_list_parent_is_a_config_error(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, {"root.yaml": "things:\n  - 1\n"})

        with pytest.raises(ConfigError) as exc_info:
            load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

        message = str(exc_info.value)
        assert "things.items" in message
        assert "root.yaml" in message

    def test_keyed_path_with_unresolvable_interpolation_is_a_config_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("LOOM_TEST_KEYED_MISSING", raising=False)
        _write_tree(
            tmp_path, {"root.yaml": "things:\n  items: ${oc.env:LOOM_TEST_KEYED_MISSING}\n"}
        )

        with pytest.raises(ConfigError) as exc_info:
            load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

        message = str(exc_info.value)
        assert "things.items" in message
        assert "root.yaml" in message


# ---------------------------------------------------------------------------
# Diamond includes — one file reached through two branches is one declaration
# ---------------------------------------------------------------------------

_DIAMOND = {
    "common.yaml": _MAPPING_A,
    "tables/orders.yaml": _includes("../common.yaml") + _MAPPING_B,
    "tables/customers.yaml": _includes("../common.yaml")
    + "things:\n  items:\n    gamma: {size: 3}\n",
    "root.yaml": _includes("tables/*.yaml"),
}


class TestDiamondKeyedIncludes:
    def test_diamond_include_with_keyed_collections_is_one_declaration(
        self, tmp_path: Path
    ) -> None:
        _write_tree(tmp_path, _DIAMOND)

        cfg = load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

        assert _as_dict(cfg) == {
            "things": {"items": {"alpha": {"size": 1}, "beta": {"size": 2}, "gamma": {"size": 3}}}
        }

    def test_diamond_include_parses_the_shared_file_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from loom.core.config import loader

        _write_tree(tmp_path, _DIAMOND)
        parsed: list[str] = []
        original = loader._parse_local_file

        def counting(path: str, label: str, omega_conf: Any) -> Any:
            parsed.append(Path(path).name)
            return original(path, label, omega_conf)

        monkeypatch.setattr(loader, "_parse_local_file", counting)

        load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

        assert parsed.count("common.yaml") == 1

    def test_diamond_include_without_keyed_is_unchanged(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, _DIAMOND)

        cfg = load_config(str(tmp_path / "root.yaml"))

        assert _as_dict(cfg) == {
            "things": {"items": {"alpha": {"size": 1}, "beta": {"size": 2}, "gamma": {"size": 3}}}
        }

    def test_diamond_include_still_reports_a_duplicate_from_another_file(
        self, tmp_path: Path
    ) -> None:
        _write_tree(tmp_path, {**_DIAMOND, "tables/extra.yaml": _MAPPING_A_DUP})

        with pytest.raises(ConfigError, match=r"things\.items\['alpha'\]"):
            load_config(str(tmp_path / "root.yaml"), keyed=KEYED)

    def test_circular_include_with_keyed_collections_still_raises(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "a.yaml": _includes("b.yaml") + _MAPPING_A,
                "b.yaml": _includes("a.yaml") + _MAPPING_B,
            },
        )

        with pytest.raises(ConfigError, match="[Cc]ircular"):
            load_config(str(tmp_path / "a.yaml"), keyed=KEYED)
