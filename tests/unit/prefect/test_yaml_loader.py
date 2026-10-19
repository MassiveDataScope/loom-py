"""Tests for ``loom.prefect._flow_yaml.read_yaml`` (delegates to
``loom.core.config.load_config``).
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from loom.core.config import ConfigError
from loom.prefect._flow_yaml import read_yaml


def _write(tmp_path: Path, name: str, body: str) -> Path:
    p = tmp_path / name
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return p


def test_read_yaml_returns_top_level_mapping(tmp_path: Path) -> None:
    cfg = _write(tmp_path, "etl.yaml", "etl: foo\nparams:\n  a: 1\n")
    out = read_yaml(str(cfg))
    assert out["etl"] == "foo"
    assert out["params"] == {"a": 1}


def test_read_yaml_applies_single_include(tmp_path: Path) -> None:
    _write(tmp_path, "base.yaml", "params:\n  shared: x\nschedule:\n  cron: '* * * * *'\n")
    cfg = _write(
        tmp_path,
        "etl.yaml",
        "includes:\n  - base.yaml\netl: foo\nparams:\n  shared: overridden\n  extra: y\n",
    )
    out = read_yaml(str(cfg))
    assert out["etl"] == "foo"
    assert out["schedule"] == {"cron": "* * * * *"}
    assert out["params"] == {"shared": "overridden", "extra": "y"}
    assert "includes" not in out


def test_read_yaml_includes_are_recursive(tmp_path: Path) -> None:
    _write(tmp_path, "grand.yaml", "schedule:\n  cron: '0 0 * * *'\nparams:\n  a: 1\n  b: 2\n")
    _write(tmp_path, "parent.yaml", "includes: [grand.yaml]\nparams:\n  b: 20\n  c: 3\n")
    cfg = _write(tmp_path, "child.yaml", "includes: [parent.yaml]\netl: foo\nparams:\n  c: 30\n")
    out = read_yaml(str(cfg))
    assert out["etl"] == "foo"
    assert out["schedule"] == {"cron": "0 0 * * *"}
    assert out["params"] == {"a": 1, "b": 20, "c": 30}


def test_read_yaml_detects_include_cycle(tmp_path: Path) -> None:
    _write(tmp_path, "a.yaml", "includes: [b.yaml]\nparams: {a: 1}\n")
    _write(tmp_path, "b.yaml", "includes: [a.yaml]\nparams: {b: 2}\n")
    with pytest.raises(Exception, match="[Cc]ircular"):
        read_yaml(str(tmp_path / "a.yaml"))


def test_read_yaml_includes_relative_to_declaring_file(tmp_path: Path) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    _write(tmp_path, "root.yaml", "params:\n  inherited: from-root\n")
    _write(nested, "mid.yaml", "includes: [../root.yaml]\nparams:\n  mid: middle\n")
    cfg = _write(nested, "leaf.yaml", "includes: [mid.yaml]\netl: foo\n")
    out = read_yaml(str(cfg))
    assert out["params"] == {"inherited": "from-root", "mid": "middle"}


def test_read_yaml_resolves_oc_env_but_keeps_loom_placeholders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("WEBHOOK_URL", "https://hooks.example/X")
    cfg = _write(
        tmp_path,
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


def test_read_yaml_oc_env_default_when_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MISSING_VAR", raising=False)
    cfg = _write(
        tmp_path,
        "env.yaml",
        "notifications:\n  - kind: slack\n    webhook_url: ${oc.env:MISSING_VAR,}\n",
    )
    out = read_yaml(str(cfg))
    assert out["notifications"][0]["webhook_url"] == ""


def test_read_yaml_accepts_cloud_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    import io

    import fsspec

    yaml_body = "etl: from-s3\ntags: [remote]\nparams: {a: 1}\n"

    class _FakeFile(io.StringIO):
        def __enter__(self) -> _FakeFile:  # type: ignore[override]
            return self

        def __exit__(self, *_: object) -> None:
            return None

    def _fake_open(uri: str, mode: str = "r", encoding: str = "utf-8") -> _FakeFile:
        assert uri.startswith("s3://"), f"expected cloud URI, got {uri!r}"
        return _FakeFile(yaml_body)

    monkeypatch.setattr(fsspec, "open", _fake_open)
    out = read_yaml("s3://my-bucket/loom/etls/from_s3.yaml")
    assert out["etl"] == "from-s3"
    assert out["tags"] == ["remote"]
    assert out["params"] == {"a": 1}


def test_read_yaml_rejects_non_mapping_top_level(tmp_path: Path) -> None:
    cfg = _write(tmp_path, "list.yaml", "- item1\n- item2\n")
    with pytest.raises(ValueError, match="mapping"):
        read_yaml(str(cfg))


def test_read_yaml_duplicate_key_raises_config_error_naming_the_file(tmp_path: Path) -> None:
    path = tmp_path / "dup.yaml"
    path.write_text("etl: a\netl: b\n", encoding="utf-8")
    with pytest.raises(ConfigError, match=str(path)):
        read_yaml(str(path))


def test_loom_overrides_do_not_touch_a_flow_definition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write(tmp_path, "flow.yaml", "params:\n  source: web\ntags: [a]\n")
    monkeypatch.setenv("LOOM__PARAMS__SOURCE", "batch")

    assert read_yaml(str(path))["params"] == {"source": "web"}
