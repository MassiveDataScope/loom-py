"""Unit tests for ConfigContext."""

from __future__ import annotations

import pytest
from omegaconf import OmegaConf

from loom.core.config import ConfigContext, ConfigError, StructBinder
from loom.core.config.configurable import Configurable
from loom.core.model import LoomFrozenStruct


class _DbConfig(LoomFrozenStruct, frozen=True):
    host: str
    port: int = 5432


class _ServiceConfig(LoomFrozenStruct, frozen=True):
    timeout_s: float
    retries: int = 3


class _Client:
    def __init__(self, db: _DbConfig, *, label: str = "default") -> None:
        self.db = db
        self.label = label


class _Worker(Configurable):
    def __init__(self, *, queue: str, concurrency: int = 4) -> None:
        self.queue = queue
        self.concurrency = concurrency


@pytest.fixture
def ctx() -> ConfigContext:
    cfg = OmegaConf.create(
        {
            "database": {"host": "pg", "port": 5432},
            "service": {"timeout_s": 5.0, "retries": 2},
            "worker": {"queue": "orders", "concurrency": 8},
        }
    )
    return ConfigContext(cfg)


class TestConfigContextSection:
    def test_extracts_typed_section(self, ctx: ConfigContext) -> None:
        db = ctx.section("database", _DbConfig)
        assert isinstance(db, _DbConfig)
        assert db.host == "pg"
        assert db.port == 5432

    def test_raises_config_error_for_missing_key(self, ctx: ConfigContext) -> None:
        with pytest.raises(ConfigError):
            ctx.section("missing.key", _DbConfig)


class TestConfigContextBind:
    def test_bind_with_path(self, ctx: ConfigContext) -> None:
        worker = ctx.bind(_Worker, path="worker")
        assert worker.queue == "orders"
        assert worker.concurrency == 8

    def test_bind_overrides_win_over_yaml(self, ctx: ConfigContext) -> None:
        worker = ctx.bind(_Worker, path="worker", concurrency=2)
        assert worker.concurrency == 2
        assert worker.queue == "orders"

    def test_bind_no_path_uses_overrides_only(self, ctx: ConfigContext) -> None:
        worker = ctx.bind(_Worker, queue="events")
        assert worker.queue == "events"
        assert worker.concurrency == 4

    def test_bind_struct_field_converted_from_dict(self, ctx: ConfigContext) -> None:
        client = ctx.bind(_Client, db={"host": "replica", "port": 5433})
        assert isinstance(client.db, _DbConfig)
        assert client.db.port == 5433

    def test_bind_raises_on_missing_required(self, ctx: ConfigContext) -> None:
        with pytest.raises(ConfigError, match="requires config field 'queue'"):
            ctx.bind(_Worker)

    def test_bind_raises_on_bad_section(self, ctx: ConfigContext) -> None:
        with pytest.raises(ConfigError):
            ctx.bind(_Worker, path="missing.section")


class TestConfigContextResolve:
    def test_resolve_from_config_binding(self, ctx: ConfigContext) -> None:
        binding = _Worker.from_config("worker")
        worker = ctx.resolve(binding)
        assert isinstance(worker, _Worker)
        assert worker.queue == "orders"

    def test_resolve_overrides_win(self, ctx: ConfigContext) -> None:
        binding = _Worker.from_config("worker", concurrency=1)
        worker = ctx.resolve(binding)
        assert isinstance(worker, _Worker)
        assert worker.concurrency == 1

    def test_resolve_configure_no_path(self, ctx: ConfigContext) -> None:
        binding = _Worker.configure(queue="audit", concurrency=2)
        worker = ctx.resolve(binding)
        assert isinstance(worker, _Worker)
        assert worker.queue == "audit"
        assert worker.concurrency == 2


class TestConfigContextCustomBinder:
    def test_accepts_strict_binder(self) -> None:
        cfg = OmegaConf.create({"worker": {"queue": "q", "concurrency": "not-an-int"}})
        strict_ctx = ConfigContext(cfg, binder=StructBinder(strict=True))
        with pytest.raises(ConfigError):
            strict_ctx.bind(_Worker, path="worker")


class TestConfigContextRaw:
    def test_a_value_is_read_as_written_without_resolving_it(self) -> None:
        ctx = ConfigContext.from_dict(
            {"rest": {"docs_url": None, "secret": "${secrets:/api/key}", "auth": {"jwt": {}}}}
        )

        assert ctx.raw("rest") == {
            "docs_url": None,
            "secret": "${secrets:/api/key}",
            "auth": {"jwt": {}},
        }

    def test_an_absent_key_returns_the_default(self) -> None:
        ctx = ConfigContext.from_dict({"rest": {"title": "API"}})

        assert ctx.raw("rest.auth.jwt", "absent") == "absent"
