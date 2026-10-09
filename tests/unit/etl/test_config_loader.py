"""Tests for ETL runner config loader."""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.etl.lineage._config import ETLObservabilityConfig
from loom.etl.runner.config_loader import _load_yaml, config_dir
from loom.etl.storage._config import STORAGE_KEYED_COLLECTIONS, StorageConfig


def test_load_yaml_reads_storage_and_observability_sections(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text(
        """
storage:
  defaults:
    table_path:
      uri: /var/lib/loom/lake
observability:
  log:
    enabled: false
  lineage:
    enabled: true
    root: s3://bucket/runs
""",
        encoding="utf-8",
    )

    storage, obs = _load_yaml(str(path))

    assert isinstance(storage, StorageConfig)
    assert storage.defaults.table_path is not None
    assert storage.defaults.table_path.uri == "/var/lib/loom/lake"
    assert isinstance(obs, ETLObservabilityConfig)
    assert obs.log.enabled is False
    assert obs.lineage.enabled is True
    assert obs.lineage.root == "s3://bucket/runs"


def test_load_yaml_uses_default_observability_when_missing(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text(
        "storage:\n  defaults:\n    table_path:\n      uri: /var/lib/loom/lake\n",
        encoding="utf-8",
    )

    storage, obs = _load_yaml(str(path))

    assert isinstance(storage, StorageConfig)
    assert storage.defaults.table_path is not None
    assert storage.defaults.table_path.uri == "/var/lib/loom/lake"
    assert obs == ETLObservabilityConfig()


def test_load_yaml_raises_when_storage_key_missing(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text("observability:\n  log:\n    enabled: true\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="storage"):
        _load_yaml(str(path))


def test_load_yaml_from_cloud_uri() -> None:
    yaml_content = (
        "storage:\n"
        "  defaults:\n"
        "    table_path:\n"
        "      uri: s3://my-lake/delta\n"
        "observability:\n"
        "  log:\n"
        "    enabled: true\n"
        "  lineage:\n"
        "    enabled: false\n"
    )

    mock_open = MagicMock()
    mock_open.return_value.__enter__ = MagicMock(return_value=StringIO(yaml_content))
    mock_open.return_value.__exit__ = MagicMock(return_value=False)

    with patch("fsspec.open", mock_open):
        storage, obs = _load_yaml("s3://my-bucket/config/prod.yaml")

    assert isinstance(storage, StorageConfig)
    assert storage.defaults.table_path is not None
    assert storage.defaults.table_path.uri == "s3://my-lake/delta"
    assert obs.log.enabled is True
    mock_open.assert_called_once_with("s3://my-bucket/config/prod.yaml", mode="r", encoding="utf-8")


# ---------------------------------------------------------------------------
# Mapping form, keyed collections and lenient conversion
# ---------------------------------------------------------------------------


def _write_tree(root: Path, files: dict[str, str]) -> None:
    for name, content in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")


def test_load_yaml_mapping_form_binds_to_table_routes(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {
            "loom.yaml": (
                "storage:\n"
                "  tables:\n"
                "    sales.orders: {path: {uri: /lake/orders}}\n"
                "  files:\n"
                "    exports.daily: {path: {uri: /out/daily.csv}}\n"
            )
        },
    )

    storage, _ = _load_yaml(str(tmp_path / "loom.yaml"))

    assert [route.name for route in storage.tables] == ["sales.orders"]
    assert storage.tables[0].path is not None
    assert storage.tables[0].path.uri == "/lake/orders"
    assert [route.name for route in storage.files] == ["exports.daily"]


def test_load_yaml_mapping_form_across_includes_merges_by_key(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {
            "tables/sales.yaml": (
                "storage:\n  tables:\n    sales.orders: {ref: main.sales.orders}\n"
            ),
            "tables/billing.yaml": (
                "storage:\n  tables:\n    billing.invoices: {ref: main.billing.invoices}\n"
            ),
            "loom.yaml": "includes:\n  - tables/*.yaml\nstorage:\n  engine: polars\n",
        },
    )

    storage, _ = _load_yaml(str(tmp_path / "loom.yaml"))

    assert sorted(route.name for route in storage.tables) == ["billing.invoices", "sales.orders"]


def test_load_yaml_duplicate_key_across_includes_names_key_and_files(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {
            "a.yaml": "storage:\n  tables:\n    sales.orders: {ref: main.a.orders}\n",
            "b.yaml": "storage:\n  tables:\n    sales.orders: {ref: main.b.orders}\n",
            "loom.yaml": "includes:\n  - a.yaml\n  - b.yaml\n",
        },
    )

    with pytest.raises(ConfigError) as exc_info:
        _load_yaml(str(tmp_path / "loom.yaml"))

    message = str(exc_info.value)
    assert "storage.tables['sales.orders']" in message
    assert "a.yaml" in message
    assert "b.yaml" in message


def test_load_yaml_list_form_in_include_and_declaring_file_later_replaces(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {
            "base.yaml": "storage:\n  tables:\n    - {name: base.table, ref: main.base.table}\n",
            "loom.yaml": (
                "includes:\n  - base.yaml\n"
                "storage:\n  tables:\n    - {name: own.table, ref: main.own.table}\n"
            ),
        },
    )

    storage, _ = _load_yaml(str(tmp_path / "loom.yaml"))

    assert [route.name for route in storage.tables] == ["own.table"]


def test_load_yaml_same_table_key_twice_in_one_file_raises(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {
            "loom.yaml": (
                "storage:\n"
                "  tables:\n"
                "    sales.orders: {ref: main.a.orders}\n"
                "    sales.orders: {ref: main.b.orders}\n"
            )
        },
    )

    with pytest.raises(ConfigError, match="loom.yaml"):
        _load_yaml(str(tmp_path / "loom.yaml"))


def test_load_yaml_conflicting_inner_name_raises_value_error(tmp_path: Path) -> None:
    _write_tree(
        tmp_path,
        {"loom.yaml": "storage:\n  tables:\n    sales.orders: {name: other, ref: main.a.b}\n"},
    )

    with pytest.raises(ValueError, match=r"storage\.tables\['sales\.orders'\]"):
        _load_yaml(str(tmp_path / "loom.yaml"))


def test_load_yaml_target_file_size_from_env_interpolation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TFS", "1024")
    _write_tree(
        tmp_path,
        {
            "loom.yaml": (
                "storage:\n"
                "  tables:\n"
                "    sales.orders:\n"
                "      path:\n"
                "        uri: /lake/orders\n"
                "        target_file_size: ${oc.env:TFS}\n"
            )
        },
    )

    storage, _ = _load_yaml(str(tmp_path / "loom.yaml"))

    assert storage.tables[0].path is not None
    assert storage.tables[0].path.target_file_size == 1024


@pytest.mark.parametrize(
    "storage_yaml",
    [
        "storage:\n  defaults:\n    table_path:\n      uri: 123\n",
        "storage:\n  tables: x\n",
        "storage:\n  defaults: 3\n",
        "storage:\n  tables:\n    t: 3\n",
    ],
)
def test_load_yaml_invalid_shape_raises_config_error(tmp_path: Path, storage_yaml: str) -> None:
    _write_tree(tmp_path, {"loom.yaml": storage_yaml})

    with pytest.raises(ConfigError, match="StorageConfig"):
        _load_yaml(str(tmp_path / "loom.yaml"))


def test_load_yaml_forwards_resolvers_and_keeps_keyed_collections(tmp_path: Path) -> None:
    path = tmp_path / "loom.yaml"
    path.write_text("storage:\n  defaults:\n    table_path:\n      uri: /lake\n", encoding="utf-8")
    resolver = MagicMock()
    resolver.name = "stub"

    with patch(
        "loom.etl.runner.config_loader.ConfigContext.from_yaml",
        wraps=ConfigContext.from_yaml,
    ) as from_yaml:
        _load_yaml(str(path), resolvers=[resolver])

    kwargs = from_yaml.call_args.kwargs
    assert kwargs["keyed"] == STORAGE_KEYED_COLLECTIONS
    assert kwargs["resolvers"][0] is resolver


class TestConfigDir:
    def test_a_local_config_anchors_relative_paths_to_its_own_directory(
        self, tmp_path: Path
    ) -> None:
        assert config_dir(str(tmp_path / "conf" / "loom.yaml")) == tmp_path / "conf"

    def test_a_relative_config_path_is_made_absolute(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)

        assert config_dir("conf/loom.yaml") == tmp_path.resolve() / "conf"

    def test_a_remote_config_anchors_relative_paths_to_the_working_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)

        assert config_dir("s3://bucket/conf/loom.yaml") == tmp_path.resolve()
