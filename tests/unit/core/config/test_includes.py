"""Tests for ``includes`` resolution on any scheme, with globs."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

from loom.core.config import ConfigError, expand_config_glob, load_config
from loom.core.config._includes import canonical_key, resolve_include

_PARTS = {"parts/b.yaml": "b: 1\n", "parts/a.yaml": "a: 1\n", "parts/c.yaml": "c: 1\nb: 0\n"}


def _write_tree(root: Path, files: dict[str, str]) -> None:
    for name, content in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)


def _pipe_tree(fs: Any, root: str, files: dict[str, str]) -> None:
    for name, content in files.items():
        fs.pipe(f"{root}/{name}", content.encode())


def _as_dict(cfg: Any) -> Any:
    from omegaconf import OmegaConf

    return OmegaConf.to_container(cfg, resolve=True)


# ---------------------------------------------------------------------------
# US1 scenario 1 — local glob, lexicographic order, own keys on top
# ---------------------------------------------------------------------------


class TestLocalIncludeGlobs:
    def test_local_glob_merges_matches_in_order_then_own_keys(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, {**_PARTS, "a.yaml": "includes:\n  - parts/*.yaml\nc: 2\n"})

        cfg = load_config(str(tmp_path / "a.yaml"))

        assert _as_dict(cfg) == {"a": 1, "b": 0, "c": 2}

    def test_local_glob_ignores_directories_and_non_yaml_files(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, {**_PARTS, "a.yaml": "includes:\n  - parts/*\n"})
        (tmp_path / "parts" / "dir.yaml").mkdir()
        (tmp_path / "parts" / "notes.txt").write_text("x: 9\n")

        cfg = load_config(str(tmp_path / "a.yaml"))

        assert _as_dict(cfg) == {"a": 1, "b": 0, "c": 1}

    def test_local_glob_accepts_yml_suffix(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, {"parts/x.yml": "x: 1\n", "a.yaml": "includes:\n  - parts/*\n"})

        cfg = load_config(str(tmp_path / "a.yaml"))

        assert _as_dict(cfg) == {"x": 1}

    def test_absolute_local_glob_entry(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, _PARTS)
        other = tmp_path / "elsewhere"
        other.mkdir()
        (other / "a.yaml").write_text(f"includes:\n  - {tmp_path / 'parts' / '*.yaml'}\n")

        cfg = load_config(str(other / "a.yaml"))

        assert _as_dict(cfg) == {"a": 1, "b": 0, "c": 1}

    def test_parent_relative_glob_entry(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {"shared/s.yaml": "s: 1\n", "app/a.yaml": "includes:\n  - ../shared/*.yaml\n"},
        )

        cfg = load_config(str(tmp_path / "app" / "a.yaml"))

        assert _as_dict(cfg) == {"s": 1}


class TestMemoryAndCloudIncludeGlobs:
    def test_memory_uri_glob_equals_local_result(self, tmp_path: Path, memfs: Any) -> None:
        tree = {**_PARTS, "a.yaml": "includes:\n  - parts/*.yaml\nc: 2\n"}
        _write_tree(tmp_path, tree)
        _pipe_tree(memfs, "cfg", tree)

        local = load_config(str(tmp_path / "a.yaml"))
        cloud = load_config("memory://cfg/a.yaml")

        assert _as_dict(cloud) == _as_dict(local) == {"a": 1, "b": 0, "c": 2}

    def test_cloud_plain_include_resolves_relative_to_including_uri(self, memfs: Any) -> None:
        _pipe_tree(
            memfs,
            "cfg",
            {"app/a.yaml": "includes:\n  - ../base.yaml\nname: app\n", "base.yaml": "pool: 3\n"},
        )

        cfg = load_config("memory://cfg/app/a.yaml")

        assert _as_dict(cfg) == {"pool": 3, "name": "app"}

    def test_cloud_glob_ignores_directories_and_non_yaml_files(self, memfs: Any) -> None:
        _pipe_tree(memfs, "cfg", {**_PARTS, "a.yaml": "includes:\n  - parts/*\n"})
        memfs.mkdir("cfg/parts/dir.yaml")
        memfs.pipe("cfg/parts/notes.txt", b"x: 9\n")

        cfg = load_config("memory://cfg/a.yaml")

        assert _as_dict(cfg) == {"a": 1, "b": 0, "c": 1}


class TestMixedSchemeIncludes:
    def test_local_file_includes_cloud_uri(self, tmp_path: Path, memfs: Any) -> None:
        memfs.pipe("cfg/remote.yaml", b"remote: 1\n")
        (tmp_path / "a.yaml").write_text("includes:\n  - memory://cfg/remote.yaml\nlocal: 1\n")

        cfg = load_config(str(tmp_path / "a.yaml"))

        assert _as_dict(cfg) == {"remote": 1, "local": 1}

    def test_cloud_file_includes_absolute_local_path(self, tmp_path: Path, memfs: Any) -> None:
        (tmp_path / "local.yaml").write_text("local: 1\n")
        memfs.pipe("cfg/a.yaml", f"includes:\n  - {tmp_path / 'local.yaml'}\nremote: 1\n".encode())

        cfg = load_config("memory://cfg/a.yaml")

        assert _as_dict(cfg) == {"local": 1, "remote": 1}

    def test_cloud_file_includes_local_glob(self, tmp_path: Path, memfs: Any) -> None:
        _write_tree(tmp_path, _PARTS)
        memfs.pipe("cfg/a.yaml", f"includes:\n  - {tmp_path / 'parts' / '*.yaml'}\n".encode())

        cfg = load_config("memory://cfg/a.yaml")

        assert _as_dict(cfg) == {"a": 1, "b": 0, "c": 1}


class TestIncludeMisses:
    def test_local_glob_with_no_match_raises(self, tmp_path: Path) -> None:
        main = tmp_path / "a.yaml"
        main.write_text("includes:\n  - parts/*.yaml\n")

        with pytest.raises(ConfigError) as info:
            load_config(str(main))

        assert "parts/*.yaml" in str(info.value)
        assert f"included from {str(main)!r}" in str(info.value)

    def test_local_glob_matching_only_non_yaml_is_no_match(self, tmp_path: Path) -> None:
        (tmp_path / "parts").mkdir()
        (tmp_path / "parts" / "notes.txt").write_text("x: 1\n")
        main = tmp_path / "a.yaml"
        main.write_text("includes:\n  - parts/*\n")

        with pytest.raises(ConfigError, match="matches no configuration file"):
            load_config(str(main))

    def test_local_plain_include_missing_keeps_not_found_and_names_declaring(
        self,
        tmp_path: Path,
    ) -> None:
        main = tmp_path / "a.yaml"
        main.write_text("includes:\n  - nonexistent.yaml\n")

        with pytest.raises(ConfigError) as info:
            load_config(str(main))

        message = str(info.value)
        assert "Configuration file not found" in message
        assert "nonexistent.yaml" in message
        assert f"(included from {str(main)!r})" in message

    @pytest.mark.parametrize(
        ("document", "expected"),
        [
            pytest.param(
                b"includes:\n  - parts/*.yaml\n",
                "memory://cfg/parts/*.yaml",
                id="cloud_glob_with_no_match_raises",
            ),
            pytest.param(
                b"includes:\n  - missing.yaml\n",
                "memory://cfg/missing.yaml",
                id="cloud_plain_include_missing_names_declaring",
            ),
        ],
    )
    def test_cloud_missing_include_raises_naming_it(
        self, document: bytes, expected: str, memfs: Any
    ) -> None:
        memfs.pipe("cfg/a.yaml", document)

        with pytest.raises(ConfigError) as info:
            load_config("memory://cfg/a.yaml")

        assert expected in str(info.value)
        assert "included from 'memory://cfg/a.yaml'" in str(info.value)

    def test_included_parse_failure_names_declaring(self, memfs: Any) -> None:
        memfs.pipe("cfg/a.yaml", b"includes:\n  - broken.yaml\n")
        memfs.pipe("cfg/broken.yaml", b"key: [unclosed\n")

        with pytest.raises(ConfigError) as info:
            load_config("memory://cfg/a.yaml")

        assert "Failed to parse" in str(info.value)
        assert "included from 'memory://cfg/a.yaml'" in str(info.value)

    def test_nested_include_error_names_the_direct_declaring_file(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path, {"a.yaml": "includes:\n  - b.yaml\n", "b.yaml": "includes:\n  - c.yaml\n"}
        )

        with pytest.raises(ConfigError) as info:
            load_config(str(tmp_path / "a.yaml"))

        assert f"included from {str(tmp_path / 'b.yaml')!r}" in str(info.value)


class TestCloudIncludeCycles:
    def test_cloud_circular_include_raises(self, memfs: Any) -> None:
        _pipe_tree(
            memfs, "cfg", {"a.yaml": "includes:\n  - b.yaml\n", "b.yaml": "includes:\n  - a.yaml\n"}
        )

        with pytest.raises(ConfigError, match="[Cc]ircular"):
            load_config("memory://cfg/a.yaml")

    def test_cloud_cycle_detected_through_parent_segment(self, memfs: Any) -> None:
        _pipe_tree(
            memfs,
            "cfg",
            {"a.yaml": "includes:\n  - x/b.yaml\n", "x/b.yaml": "includes:\n  - ../x/../a.yaml\n"},
        )

        with pytest.raises(ConfigError, match="[Cc]ircular"):
            load_config("memory://cfg/a.yaml")

    def test_canonical_key_normalises_cloud_path(self) -> None:
        assert (
            canonical_key("s3://b/x/../a.yaml") == canonical_key("s3://b/a.yaml") == "s3://b/a.yaml"
        )

    def test_canonical_key_equates_empty_netloc_and_netloc_forms(self) -> None:
        assert (
            canonical_key("memory:///cfg/x.yaml")
            == canonical_key("memory://cfg/x.yaml")
            == "memory://cfg/x.yaml"
        )

    def test_cloud_cycle_through_glob_matched_file_is_detected_at_first_repeat(
        self, memfs: Any
    ) -> None:
        _pipe_tree(
            memfs,
            "cfg",
            {
                "a.yaml": "includes:\n  - parts/*.yaml\n",
                "parts/b.yaml": "includes:\n  - ../a.yaml\n",
            },
        )

        with pytest.raises(ConfigError, match=r"Circular include detected: 'memory:/+cfg/a\.yaml'"):
            load_config("memory://cfg/a.yaml")

    def test_canonical_key_resolves_local_path(self, tmp_path: Path) -> None:
        assert canonical_key(str(tmp_path / "x" / ".." / "a.yaml")) == str(
            (tmp_path / "a.yaml").resolve()
        )


class TestLocalIncludesWithoutFsspec:
    def test_local_includes_and_globs_work_without_fsspec(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_tree(
            tmp_path,
            {
                **_PARTS,
                "base.yaml": "base: 1\n",
                "a.yaml": "includes:\n  - base.yaml\n  - parts/*.yaml\n",
            },
        )
        monkeypatch.setitem(sys.modules, "fsspec", None)
        monkeypatch.setitem(sys.modules, "fsspec.core", None)

        cfg = load_config(str(tmp_path / "a.yaml"))

        assert _as_dict(cfg) == {"base": 1, "a": 1, "b": 0, "c": 1}


class _SentinelResolver:
    @property
    def name(self) -> str:
        return "secrets"

    def resolve(self, key: str) -> object:
        return "SENTINEL"


class TestIncludeErrorsNameTheLiteralEntry:
    def test_include_error_names_literal_entry_not_resolved_secret(self, tmp_path: Path) -> None:
        main = tmp_path / "a.yaml"
        main.write_text("includes:\n  - ${secrets:/x}/base.yaml\n")
        main_path = str(main)
        resolvers = [_SentinelResolver()]

        with pytest.raises(ConfigError) as info:
            load_config(main_path, resolvers=resolvers)

        message = str(info.value)
        assert "SENTINEL" not in message
        assert "'${secrets:/x}/base.yaml'" in message
        assert f"(included from {main_path!r})" in message

    def test_glob_no_match_names_literal_entry_not_resolved_secret(self, tmp_path: Path) -> None:
        main = tmp_path / "a.yaml"
        main.write_text("includes:\n  - ${secrets:/x}/*.yaml\n")
        main_path = str(main)
        resolvers = [_SentinelResolver()]

        with pytest.raises(ConfigError) as info:
            load_config(main_path, resolvers=resolvers)

        message = str(info.value)
        assert "SENTINEL" not in message
        assert "Include '${secrets:/x}/*.yaml' matches no configuration file" in message

    def test_parse_error_in_plain_glob_match_names_that_file(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {"a.yaml": "includes:\n  - parts/*.yaml\n", "parts/ok.yaml": "x: 1\n"},
        )
        broken = tmp_path / "parts" / "broken.yaml"
        broken.write_text("key: [unclosed\n")

        with pytest.raises(ConfigError) as info:
            load_config(str(tmp_path / "a.yaml"))

        assert f"Failed to parse configuration file {str(broken)!r}" in str(info.value)

    def test_parse_error_behind_interpolated_include_hides_resolved_path(
        self, tmp_path: Path
    ) -> None:
        _write_tree(
            tmp_path,
            {
                "SENTINEL/base.yaml": "key: [unclosed\n",
                "a.yaml": "includes:\n  - ${secrets:/x}/base.yaml\n",
            },
        )
        main_path = str(tmp_path / "a.yaml")
        resolvers = [_SentinelResolver()]

        with pytest.raises(ConfigError) as info:
            load_config(main_path, resolvers=resolvers)

        message = str(info.value)
        assert "SENTINEL" not in message
        assert "Failed to parse configuration file '${secrets:/x}/base.yaml'" in message
        assert info.value.__cause__ is not None

    def test_keyed_duplicate_across_interpolated_includes_names_literal_entries(
        self,
        tmp_path: Path,
    ) -> None:
        _write_tree(
            tmp_path,
            {
                "SENTINEL/left.yaml": "tables:\n  t: {a: 1}\n",
                "SENTINEL/right.yaml": "tables:\n  t: {a: 2}\n",
                "a.yaml": "includes:\n  - ${secrets:/x}/left.yaml\n  - ${secrets:/x}/right.yaml\n",
            },
        )
        main_path = str(tmp_path / "a.yaml")
        resolvers = [_SentinelResolver()]

        with pytest.raises(ConfigError) as info:
            load_config(main_path, resolvers=resolvers, keyed=("tables",))

        message = str(info.value)
        assert "SENTINEL" not in message
        assert "'${secrets:/x}/left.yaml'" in message
        assert "'${secrets:/x}/right.yaml'" in message


class TestIncludeEntryInterpolation:
    def test_include_entry_interpolation_resolves_before_glob(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VARIANT", "prod")
        _write_tree(
            tmp_path,
            {"prod/x.yaml": "x: 1\n", "a.yaml": "includes:\n  - ${oc.env:VARIANT}/*.yaml\n"},
        )

        cfg = load_config(str(tmp_path / "a.yaml"))

        assert _as_dict(cfg) == {"x": 1}


class TestResolveInclude:
    def test_resolve_include_keeps_scheme_and_absolute_entries(self, tmp_path: Path) -> None:
        assert resolve_include("s3://b/a.yaml", "gs://o/x.yaml") == "gs://o/x.yaml"
        assert resolve_include("s3://b/a.yaml", "/etc/x.yaml") == "/etc/x.yaml"
        assert resolve_include(str(tmp_path / "a.yaml"), "s3://b/x.yaml") == "s3://b/x.yaml"

    def test_resolve_include_joins_relative_entries(self, tmp_path: Path) -> None:
        assert resolve_include("s3://b/x/a.yaml", "../parts/*.yaml") == "s3://b/parts/*.yaml"
        assert resolve_include(str(tmp_path / "x" / "a.yaml"), "../p/*.yaml") == str(
            tmp_path / "p" / "*.yaml"
        )


class TestExpandConfigGlob:
    def test_expand_config_glob_returns_plain_entry_untouched(self, tmp_path: Path) -> None:
        assert expand_config_glob("s3://b/a.yaml") == ["s3://b/a.yaml"]
        assert expand_config_glob(str(tmp_path / "missing.yaml")) == [
            str(tmp_path / "missing.yaml")
        ]

    def test_expand_config_glob_local_sorted(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, _PARTS)

        assert expand_config_glob(str(tmp_path / "parts" / "*.yaml")) == [
            str(tmp_path / "parts" / "a.yaml"),
            str(tmp_path / "parts" / "b.yaml"),
            str(tmp_path / "parts" / "c.yaml"),
        ]

    def test_expand_config_glob_raises_on_no_match(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="matches no configuration file"):
            expand_config_glob(str(tmp_path / "*.yaml"))

    def test_cloud_glob_sorts_and_restores_protocol_on_bare_paths(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        pytest.importorskip("fsspec")
        from fsspec.spec import AbstractFileSystem

        class FakeFileSystem(AbstractFileSystem):  # type: ignore[misc]
            protocol = "fake"

            def glob(
                self, path: str, maxdepth: int | None = None, **kwargs: Any
            ) -> dict[str, dict[str, Any]]:
                return {
                    "cfg/parts/b.yaml": {"type": "file"},
                    "cfg/parts/a.yml": {"type": "file"},
                    "cfg/parts/sub": {"type": "directory"},
                    "cfg/parts/c.txt": {"type": "file"},
                }

        fake = FakeFileSystem()
        monkeypatch.setattr("fsspec.core.url_to_fs", lambda uri, **kw: (fake, "cfg/parts/*"))

        assert expand_config_glob("s3://cfg/parts/*") == [
            "fake://cfg/parts/a.yml",
            "fake://cfg/parts/b.yaml",
        ]

    def test_cloud_glob_wraps_backend_errors_naming_uri(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pytest.importorskip("fsspec")

        def boom(uri: str, **kw: Any) -> Any:
            raise PermissionError("access denied")

        monkeypatch.setattr("fsspec.core.url_to_fs", boom)

        with pytest.raises(ConfigError, match=r"s3://locked/\*\.yaml.*access denied"):
            expand_config_glob("s3://locked/*.yaml")
