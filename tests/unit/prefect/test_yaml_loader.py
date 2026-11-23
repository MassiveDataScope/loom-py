"""Tests for ``loom.prefect._flow_yaml.read_yaml`` (delegates to
``loom.core.config.load_config``).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from loom.core.config import ConfigError
from loom.prefect._flow_yaml import read_yaml


class TestReadYaml:
    def test_returns_the_top_level_mapping(
        self, write_yaml: Callable[[str, str], Path], tmp_path: Path
    ) -> None:
        cfg = write_yaml("etl.yaml", "etl: foo\nparams:\n  a: 1\n")
        out = read_yaml(str(cfg))
        assert out["etl"] == "foo"
        assert out["params"] == {"a": 1}

    def test_applies_a_single_include(
        self, write_yaml: Callable[[str, str], Path], tmp_path: Path
    ) -> None:
        write_yaml("base.yaml", "params:\n  shared: x\nschedule:\n  cron: '* * * * *'\n")
        cfg = write_yaml(
            "etl.yaml",
            "includes:\n  - base.yaml\netl: foo\nparams:\n  shared: overridden\n  extra: y\n",
        )
        out = read_yaml(str(cfg))
        assert out["etl"] == "foo"
        assert out["schedule"] == {"cron": "* * * * *"}
        assert out["params"] == {"shared": "overridden", "extra": "y"}
        assert "includes" not in out

    def test_includes_are_recursive(
        self, write_yaml: Callable[[str, str], Path], tmp_path: Path
    ) -> None:
        write_yaml("grand.yaml", "schedule:\n  cron: '0 0 * * *'\nparams:\n  a: 1\n  b: 2\n")
        write_yaml("parent.yaml", "includes: [grand.yaml]\nparams:\n  b: 20\n  c: 3\n")
        cfg = write_yaml("child.yaml", "includes: [parent.yaml]\netl: foo\nparams:\n  c: 30\n")
        out = read_yaml(str(cfg))
        assert out["etl"] == "foo"
        assert out["schedule"] == {"cron": "0 0 * * *"}
        assert out["params"] == {"a": 1, "b": 20, "c": 30}

    def test_detects_an_include_cycle(
        self, write_yaml: Callable[[str, str], Path], tmp_path: Path
    ) -> None:
        write_yaml("a.yaml", "includes: [b.yaml]\nparams: {a: 1}\n")
        write_yaml("b.yaml", "includes: [a.yaml]\nparams: {b: 2}\n")
        with pytest.raises(Exception, match="[Cc]ircular"):
            read_yaml(str(tmp_path / "a.yaml"))

    def test_includes_are_relative_to_the_declaring_file(
        self, write_yaml: Callable[[str, str], Path], tmp_path: Path
    ) -> None:
        nested = tmp_path / "nested"
        nested.mkdir()
        write_yaml("root.yaml", "params:\n  inherited: from-root\n")
        write_yaml("nested/mid.yaml", "includes: [../root.yaml]\nparams:\n  mid: middle\n")
        cfg = write_yaml("nested/leaf.yaml", "includes: [mid.yaml]\netl: foo\n")
        out = read_yaml(str(cfg))
        assert out["params"] == {"inherited": "from-root", "mid": "middle"}

    def test_resolves_oc_env_but_keeps_loom_placeholders(
        self,
        write_yaml: Callable[[str, str], Path],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("WEBHOOK_URL", "https://hooks.example/X")
        cfg = write_yaml(
            "mixed.yaml",
            """
            params:
              start: ${now-1d}
              end: ${now}
            notifications:
              - kind: slack
                webhook_url: ${oc.env:WEBHOOK_URL,}
            """,
        )
        out = read_yaml(str(cfg))
        # Loom placeholders preserved verbatim for runtime resolution.
        assert out["params"]["start"] == "${now-1d}"
        assert out["params"]["end"] == "${now}"
        # ${oc.env:...} resolved at load time against the host env.
        assert out["notifications"][0]["webhook_url"] == "https://hooks.example/X"

    def test_oc_env_default_when_missing(
        self,
        write_yaml: Callable[[str, str], Path],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        cfg = write_yaml(
            "env.yaml",
            "notifications:\n  - kind: slack\n    webhook_url: ${oc.env:MISSING_VAR,}\n",
        )
        out = read_yaml(str(cfg))
        assert out["notifications"][0]["webhook_url"] == ""

    def test_accepts_a_cloud_uri(self, memfs: Any) -> None:
        yaml_body = "etl: from-s3\ntags: [remote]\nparams: {a: 1}\n"
        memfs.pipe("my-bucket/loom/etls/from_s3.yaml", yaml_body.encode())

        out = read_yaml("memory://my-bucket/loom/etls/from_s3.yaml")

        assert out["etl"] == "from-s3"
        assert out["tags"] == ["remote"]
        assert out["params"] == {"a": 1}

    def test_rejects_a_non_mapping_top_level(
        self, write_yaml: Callable[[str, str], Path], tmp_path: Path
    ) -> None:
        cfg = write_yaml("list.yaml", "- item1\n- item2\n")
        with pytest.raises(ConfigError, match="mapping"):
            read_yaml(str(cfg))

    def test_duplicate_key_raises_config_error_naming_the_file(self, tmp_path: Path) -> None:
        path = tmp_path / "dup.yaml"
        path.write_text("etl: a\netl: b\n", encoding="utf-8")
        with pytest.raises(ConfigError, match=str(path)):
            read_yaml(str(path))

    def test_loom_overrides_do_not_touch_a_flow_definition(
        self,
        write_yaml: Callable[[str, str], Path],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        path = write_yaml("flow.yaml", "params:\n  source: web\ntags: [a]\n")
        monkeypatch.setenv("LOOM__PARAMS__SOURCE", "batch")

        assert read_yaml(str(path))["params"] == {"source": "web"}
