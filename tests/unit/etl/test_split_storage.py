"""End-to-end equality of a split ``storage:`` tree against its single-file original."""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import msgspec
import pytest
import yaml
from omegaconf import OmegaConf

from loom.core.config import ConfigError
from loom.etl.runner.config_loader import _load_yaml
from loom.etl.storage import StorageConfig

_FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "config" / "split_storage"
_SPLIT_FILES = ("storage.yaml", "common.yaml", "tables/sales.yaml", "tables/billing.yaml")
_EXPECTED_TABLES = (
    "billing.invoices",
    "billing.payments",
    "billing.accounts",
    "sales.orders",
    "sales.customers",
    "sales.order_lines",
    "sales.regions",
)


def _load(path: str) -> StorageConfig:
    storage, _ = _load_yaml(path)
    storage.validate()
    return storage


def _routes(config: StorageConfig) -> StorageConfig:
    """Return ``config`` without ``profiles``, which the list-form original never declares."""
    return msgspec.structs.replace(config, profiles={})


def _rewrite_tables(path: Path, edit: Any) -> None:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    edit(document["storage"]["tables"])
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


@pytest.fixture
def split_tree(tmp_path: Path) -> Path:
    root = tmp_path / "split"
    shutil.copytree(_FIXTURE, root)
    return root


@pytest.fixture
def memfs() -> Iterator[Any]:
    fsspec = pytest.importorskip("fsspec")
    fs = fsspec.filesystem("memory")
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]
    yield fs
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]


# ---------------------------------------------------------------------------
# SC-001 — split tree equals the original, from disk and from memory://
# ---------------------------------------------------------------------------


def test_original_fixture_declares_the_expected_routes() -> None:
    original = _load(str(_FIXTURE / "original.yaml"))

    assert tuple(route.name for route in original.tables) == _EXPECTED_TABLES
    assert tuple(route.name for route in original.files) == ("billing.statements", "sales.exports")
    assert original.profiles == {}


def test_split_tree_on_disk_equals_the_original() -> None:
    original = _load(str(_FIXTURE / "original.yaml"))
    split = _load(str(_FIXTURE / "storage.yaml"))

    assert set(split.profiles) == {"standard", "large"}
    assert _routes(split) == original


def test_split_tree_on_memory_equals_the_original(memfs: Any) -> None:
    for name in _SPLIT_FILES:
        memfs.pipe(f"cfg/{name}", (_FIXTURE / name).read_bytes())

    split = _load("memory://cfg/storage.yaml")

    assert _routes(split) == _load(str(_FIXTURE / "original.yaml"))


# ---------------------------------------------------------------------------
# SC-002 — removing one table and duplicating one across files
# ---------------------------------------------------------------------------


def test_removing_one_table_from_one_file_drops_exactly_that_route(split_tree: Path) -> None:
    before = _load(str(split_tree / "storage.yaml"))
    _rewrite_tables(split_tree / "tables" / "sales.yaml", lambda t: t.pop("sales.customers"))

    after = _load(str(split_tree / "storage.yaml"))

    kept = tuple(route for route in before.tables if route.name != "sales.customers")
    assert after.tables == kept
    assert msgspec.structs.replace(after, tables=before.tables) == before


def test_declaring_one_table_in_two_files_names_key_and_both_files(split_tree: Path) -> None:
    duplicate = {"path": {"uri": "s3://lake/billing/customers", "profile": "standard"}}
    _rewrite_tables(
        split_tree / "tables" / "billing.yaml",
        lambda t: t.__setitem__("sales.customers", duplicate),
    )

    with pytest.raises(ConfigError, match=r"storage\.tables\['sales\.customers'\]") as info:
        _load(str(split_tree / "storage.yaml"))

    assert "sales.yaml" in str(info.value)
    assert "billing.yaml" in str(info.value)


# ---------------------------------------------------------------------------
# SC-006 — 200 tables across four files load with each file parsed once
# ---------------------------------------------------------------------------


def _write_generated_tree(root: Path, domains: int, tables_per_domain: int) -> Path:
    (root / "tables").mkdir(parents=True)
    shutil.copy(_FIXTURE / "common.yaml", root / "common.yaml")
    for domain in range(domains):
        tables = {
            f"d{domain}.t{index}": {
                "path": {"uri": f"s3://lake/d{domain}/t{index}", "profile": "large"}
            }
            for index in range(tables_per_domain)
        }
        (root / "tables" / f"d{domain}.yaml").write_text(
            yaml.safe_dump({"includes": ["../common.yaml"], "storage": {"tables": tables}}),
            encoding="utf-8",
        )
    storage = root / "storage.yaml"
    storage.write_text(
        "includes:\n  - common.yaml\n  - tables/*.yaml\nstorage:\n  engine: polars\n",
        encoding="utf-8",
    )
    return storage


def test_two_hundred_tables_across_four_files_parse_each_file_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every file is parsed once, though ``common.yaml`` is reached from all five files.

    Counted rather than timed: a wall-clock budget measured the CI runner as
    much as the loader.
    """
    storage = _write_generated_tree(tmp_path, domains=4, tables_per_domain=50)
    parse = OmegaConf.load
    parsed: list[Path] = []

    def counting_parse(file: str) -> Any:
        parsed.append(Path(file).resolve())
        return parse(file)

    monkeypatch.setattr(OmegaConf, "load", counting_parse)

    config = _load(str(storage))

    assert len(config.tables) == 200
    assert sorted(parsed) == sorted(path.resolve() for path in tmp_path.rglob("*.yaml"))
