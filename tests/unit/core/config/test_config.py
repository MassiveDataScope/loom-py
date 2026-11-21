"""Unit tests for load_config and section helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import msgspec
import pytest

from loom.core.config.errors import ConfigError
from loom.core.config.loader import load_config, section

# ---------------------------------------------------------------------------
# User-defined config structs (no framework base class)
# ---------------------------------------------------------------------------


class DatabaseConfig(msgspec.Struct, kw_only=True):
    url: str
    pool_size: int = 5


class CacheConfig(msgspec.Struct, kw_only=True):
    host: str = "localhost"
    port: int = 6379


@dataclass
class AppSettings:
    debug: bool = False
    log_level: str = "INFO"


# ---------------------------------------------------------------------------
# load_config — single file
# ---------------------------------------------------------------------------


class TestLoadConfigFiles:
    def test_load_config_single_file(self, tmp_path: Path) -> None:
        f = tmp_path / "config.yaml"
        f.write_text("database:\n  url: sqlite:///dev.db\n  pool_size: 3\n")
        cfg = load_config(str(f))
        assert cfg.database.url == "sqlite:///dev.db"
        assert cfg.database.pool_size == 3

    def test_load_config_returns_dictconfig(self, tmp_path: Path) -> None:
        from omegaconf import DictConfig

        f = tmp_path / "cfg.yaml"
        f.write_text("key: value\n")
        cfg = load_config(str(f))
        assert isinstance(cfg, DictConfig)

    def test_load_config_dot_access(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.yaml"
        f.write_text("server:\n  host: 0.0.0.0\n  port: 8080\n")
        cfg = load_config(str(f))
        assert cfg.server.host == "0.0.0.0"
        assert cfg.server.port == 8080

    def test_load_config_multiple_files_later_overrides(self, tmp_path: Path) -> None:
        base = tmp_path / "base.yaml"
        base.write_text("debug: false\nlog_level: INFO\n")
        override = tmp_path / "local.yaml"
        override.write_text("debug: true\n")

        cfg = load_config(str(base), str(override))
        assert cfg.debug is True
        assert cfg.log_level == "INFO"

    def test_load_config_three_files_merge(self, tmp_path: Path) -> None:
        a = tmp_path / "a.yaml"
        a.write_text("x: 1\ny: 1\n")
        b = tmp_path / "b.yaml"
        b.write_text("y: 2\nz: 2\n")
        c = tmp_path / "c.yaml"
        c.write_text("z: 3\n")

        cfg = load_config(str(a), str(b), str(c))
        assert cfg.x == 1
        assert cfg.y == 2
        assert cfg.z == 3

    def test_load_config_nested_merge(self, tmp_path: Path) -> None:
        base = tmp_path / "base.yaml"
        base.write_text("database:\n  url: sqlite:///base.db\n  pool_size: 5\n")
        override = tmp_path / "prod.yaml"
        override.write_text("database:\n  url: postgresql://prod/mydb\n")

        cfg = load_config(str(base), str(override))
        assert cfg.database.url == "postgresql://prod/mydb"
        assert cfg.database.pool_size == 5  # from base


class TestLoadConfigEnvInterpolation:
    def test_load_config_env_interpolation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DB_URL", "postgresql+asyncpg://prod-host/prod")
        f = tmp_path / "cfg.yaml"
        f.write_text("database:\n  url: ${oc.env:DB_URL}\n")
        cfg = load_config(str(f))
        assert cfg.database.url == "postgresql+asyncpg://prod-host/prod"


class TestLoadConfigMissingFiles:
    def test_load_config_no_files_raises(self) -> None:
        with pytest.raises(ConfigError, match="at least one"):
            load_config()

    def test_load_config_file_not_found_raises(self) -> None:
        with pytest.raises(ConfigError, match="not found"):
            load_config("/nonexistent/path/config.yaml")

    def test_load_config_one_missing_among_many_raises(self, tmp_path: Path) -> None:
        good = tmp_path / "good.yaml"
        good.write_text("x: 1\n")
        with pytest.raises(ConfigError, match="not found"):
            load_config(str(good), "/nonexistent/missing.yaml")


class TestConfigSection:
    def test_section_msgspec_struct(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.yaml"
        f.write_text("database:\n  url: sqlite:///test.db\n  pool_size: 10\n")
        cfg = load_config(str(f))
        db = section(cfg, "database", DatabaseConfig)
        assert isinstance(db, DatabaseConfig)
        assert db.url == "sqlite:///test.db"
        assert db.pool_size == 10

    def test_section_defaults_applied(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.yaml"
        f.write_text("database:\n  url: sqlite:///dev.db\n")
        cfg = load_config(str(f))
        db = section(cfg, "database", DatabaseConfig)
        assert db.pool_size == 5  # default

    def test_section_nested_key(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.yaml"
        f.write_text("services:\n  cache:\n    host: redis-host\n    port: 6380\n")
        cfg = load_config(str(f))
        cache = section(cfg, "services.cache", CacheConfig)
        assert cache.host == "redis-host"
        assert cache.port == 6380

    def test_section_dataclass(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.yaml"
        f.write_text("app:\n  debug: true\n  log_level: DEBUG\n")
        cfg = load_config(str(f))
        app = section(cfg, "app", AppSettings)
        assert app.debug is True
        assert app.log_level == "DEBUG"

    def test_section_env_interpolation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("REDIS_HOST", "my-redis")
        f = tmp_path / "cfg.yaml"
        f.write_text("cache:\n  host: ${oc.env:REDIS_HOST}\n")
        cfg = load_config(str(f))
        cache = section(cfg, "cache", CacheConfig)
        assert cache.host == "my-redis"

    def test_section_scalar_leaf(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("API_TOKEN", "tok-123")
        f = tmp_path / "cfg.yaml"
        f.write_text("api:\n  token: ${oc.env:API_TOKEN}\n  retries: '3'\n")
        cfg = load_config(str(f))

        assert section(cfg, "api.token", str) == "tok-123"
        assert section(cfg, "api.retries", int) == 3


class TestConfigSectionErrors:
    def test_section_missing_key_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.yaml"
        f.write_text("other: value\n")
        cfg = load_config(str(f))
        with pytest.raises(ConfigError, match="not found"):
            section(cfg, "database", DatabaseConfig)

    def test_section_validation_failure_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.yaml"
        f.write_text("database:\n  pool_size: not-a-number\n")  # url missing, pool_size wrong type
        cfg = load_config(str(f))
        with pytest.raises(ConfigError, match="validation"):
            section(cfg, "database", DatabaseConfig)

    def test_section_nested_key_missing_intermediate_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.yaml"
        f.write_text("services:\n  other: value\n")
        cfg = load_config(str(f))
        with pytest.raises(ConfigError, match="not found"):
            section(cfg, "services.cache", CacheConfig)


class TestConfigIncludes:
    def test_includes_merges_base_into_declaring_file(self, tmp_path: Path) -> None:
        base = tmp_path / "base.yaml"
        base.write_text("database:\n  url: sqlite:///base.db\n  pool_size: 5\n")
        main = tmp_path / "app.yaml"
        main.write_text("includes:\n  - base.yaml\napp:\n  name: myapp\n")

        cfg = load_config(str(main))
        assert cfg.app.name == "myapp"
        assert cfg.database.url == "sqlite:///base.db"
        assert cfg.database.pool_size == 5

    def test_includes_key_stripped_from_result(self, tmp_path: Path) -> None:
        base = tmp_path / "base.yaml"
        base.write_text("x: 1\n")
        main = tmp_path / "app.yaml"
        main.write_text("includes:\n  - base.yaml\ny: 2\n")

        cfg = load_config(str(main))
        assert not hasattr(cfg, "includes")
        assert cfg.x == 1
        assert cfg.y == 2

    def test_declaring_file_overrides_included_values(self, tmp_path: Path) -> None:
        base = tmp_path / "base.yaml"
        base.write_text("database:\n  url: sqlite:///base.db\n  pool_size: 5\n")
        main = tmp_path / "app.yaml"
        main.write_text("includes:\n  - base.yaml\ndatabase:\n  url: postgresql://prod/db\n")

        cfg = load_config(str(main))
        assert cfg.database.url == "postgresql://prod/db"
        assert cfg.database.pool_size == 5  # from base

    def test_includes_multiple_files_in_order(self, tmp_path: Path) -> None:
        a = tmp_path / "a.yaml"
        a.write_text("x: 1\ny: 1\n")
        b = tmp_path / "b.yaml"
        b.write_text("y: 2\nz: 2\n")
        main = tmp_path / "app.yaml"
        main.write_text("includes:\n  - a.yaml\n  - b.yaml\nz: 3\n")

        cfg = load_config(str(main))
        assert cfg.x == 1
        assert cfg.y == 2  # b overrides a
        assert cfg.z == 3  # main overrides b

    def test_includes_resolved_relative_to_declaring_file(self, tmp_path: Path) -> None:
        sub = tmp_path / "sub"
        sub.mkdir()
        base = sub / "base.yaml"
        base.write_text("key: from_sub\n")
        main = tmp_path / "app.yaml"
        main.write_text("includes:\n  - sub/base.yaml\n")

        cfg = load_config(str(main))
        assert cfg.key == "from_sub"

    def test_includes_recursive(self, tmp_path: Path) -> None:
        grandparent = tmp_path / "grandparent.yaml"
        grandparent.write_text("level: grandparent\n")
        parent = tmp_path / "parent.yaml"
        parent.write_text("includes:\n  - grandparent.yaml\nlevel: parent\n")
        main = tmp_path / "app.yaml"
        main.write_text("includes:\n  - parent.yaml\n")

        cfg = load_config(str(main))
        assert cfg.level == "parent"  # parent overrides grandparent

    def test_includes_circular_reference_raises(self, tmp_path: Path) -> None:
        a = tmp_path / "a.yaml"
        b = tmp_path / "b.yaml"
        a.write_text("includes:\n  - b.yaml\n")
        b.write_text("includes:\n  - a.yaml\n")

        with pytest.raises(ConfigError, match="[Cc]ircular"):
            load_config(str(a))

    def test_includes_missing_file_raises(self, tmp_path: Path) -> None:
        main = tmp_path / "app.yaml"
        main.write_text("includes:\n  - nonexistent.yaml\n")

        with pytest.raises(ConfigError, match="not found"):
            load_config(str(main))

    def test_includes_resolves_env_interpolation_in_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VARIANT", "prod")
        (tmp_path / "values.prod.yaml").write_text("database:\n  url: postgresql://prod/db\n")
        main = tmp_path / "app.yaml"
        main.write_text("includes:\n  - values.${oc.env:VARIANT}.yaml\napp:\n  name: myapp\n")

        cfg = load_config(str(main))

        assert cfg.database.url == "postgresql://prod/db"
        assert cfg.app.name == "myapp"

    def test_includes_resolves_env_interpolation_default_when_var_unset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VARIANT", raising=False)
        (tmp_path / "values.local.yaml").write_text("flag: from_default\n")
        main = tmp_path / "app.yaml"
        main.write_text("includes:\n  - values.${oc.env:VARIANT,local}.yaml\n")

        cfg = load_config(str(main))

        assert cfg.flag == "from_default"


class TestLoadConfigCloudUris:
    def test_load_config_from_cloud_uri(self, memfs: Any) -> None:
        memfs.pipe(
            "my-bucket/config/prod.yaml",
            b"database:\n  url: postgresql://cloud-host/prod\n  pool_size: 10\n",
        )

        cfg = load_config("memory://my-bucket/config/prod.yaml")

        assert cfg.database.url == "postgresql://cloud-host/prod"
        assert cfg.database.pool_size == 10

    def test_load_config_cloud_uri_resolves_includes_relative_to_uri(self, memfs: Any) -> None:
        memfs.pipe(
            "my-bucket/config/prod.yaml",
            b"includes:\n  - base.yaml\ndatabase:\n  url: sqlite:///dev.db\n",
        )
        memfs.pipe(
            "my-bucket/config/base.yaml",
            b"database:\n  url: ignored\n  pool_size: 4\n",
        )

        cfg = load_config("memory://my-bucket/config/prod.yaml")

        assert cfg.database.url == "sqlite:///dev.db"
        assert cfg.database.pool_size == 4

    def test_load_config_cloud_uri_merged_with_local(self, tmp_path: Path, memfs: Any) -> None:
        local = tmp_path / "base.yaml"
        local.write_text("database:\n  url: sqlite:///base.db\n  pool_size: 5\n")
        memfs.pipe("bucket/prod.yaml", b"database:\n  url: postgresql://prod/mydb\n")

        cfg = load_config(str(local), "memory://bucket/prod.yaml")

        assert cfg.database.url == "postgresql://prod/mydb"
        assert cfg.database.pool_size == 5


class TestLoadConfigCustomResolvers:
    def test_load_config_custom_resolver(self, tmp_path: Path) -> None:
        from loom.core.config import ConfigResolver

        class FakeResolver:
            name = "fake"

            def resolve(self, key: str) -> str:
                return f"resolved:{key}"

        assert isinstance(FakeResolver(), ConfigResolver)

        f = tmp_path / "cfg.yaml"
        f.write_text("database:\n  url: ${fake:my/secret/url}\n")

        cfg = load_config(str(f), resolvers=[FakeResolver()])

        assert cfg.database.url == "resolved:my/secret/url"

    def test_load_config_resolver_registration_is_idempotent(self, tmp_path: Path) -> None:
        class FakeResolver2:
            name = "fake2"

            def resolve(self, key: str) -> str:
                return f"v:{key}"

        f = tmp_path / "cfg.yaml"
        f.write_text("x: ${fake2:key}\n")

        # Registering the same resolver twice must not raise
        load_config(str(f), resolvers=[FakeResolver2()])
        load_config(str(f), resolvers=[FakeResolver2()])
