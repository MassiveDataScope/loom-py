"""Unit tests for ConfigContext."""

from __future__ import annotations

import math
from typing import Any

import pytest
from omegaconf import OmegaConf

from loom.core.config import ConfigContext, ConfigError, StructBinder
from loom.core.config.configurable import Configurable
from loom.core.model import LoomFrozenStruct
from tests.unit.core.config.doubles import AcmeDbConfig


class _ServiceConfig(LoomFrozenStruct, frozen=True):
    timeout_s: float
    retries: int = 3


class _Client:
    def __init__(self, db: AcmeDbConfig, *, label: str = "default") -> None:
        self.db = db
        self.label = label


class AcmeQueueWorker(Configurable):
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
        db = ctx.section("database", AcmeDbConfig)
        assert isinstance(db, AcmeDbConfig)
        assert db.host == "pg"
        assert db.port == 5432

    def test_raises_config_error_for_missing_key(self, ctx: ConfigContext) -> None:
        with pytest.raises(ConfigError):
            ctx.section("missing.key", AcmeDbConfig)


class TestConfigContextBind:
    def test_bind_with_path(self, ctx: ConfigContext) -> None:
        worker = ctx.bind(AcmeQueueWorker, path="worker")
        assert worker.queue == "orders"
        assert worker.concurrency == 8

    def test_bind_overrides_win_over_yaml(self, ctx: ConfigContext) -> None:
        worker = ctx.bind(AcmeQueueWorker, path="worker", concurrency=2)
        assert worker.concurrency == 2
        assert worker.queue == "orders"

    def test_bind_no_path_uses_overrides_only(self, ctx: ConfigContext) -> None:
        worker = ctx.bind(AcmeQueueWorker, queue="events")
        assert worker.queue == "events"
        assert worker.concurrency == 4

    def test_bind_struct_field_converted_from_dict(self, ctx: ConfigContext) -> None:
        client = ctx.bind(_Client, db={"host": "replica", "port": 5433})
        assert isinstance(client.db, AcmeDbConfig)
        assert client.db.port == 5433

    def test_bind_raises_on_missing_required(self, ctx: ConfigContext) -> None:
        with pytest.raises(ConfigError, match="requires config field 'queue'"):
            ctx.bind(AcmeQueueWorker)

    def test_bind_raises_on_bad_section(self, ctx: ConfigContext) -> None:
        with pytest.raises(ConfigError):
            ctx.bind(AcmeQueueWorker, path="missing.section")


class TestConfigContextResolve:
    def test_resolve_from_config_binding(self, ctx: ConfigContext) -> None:
        binding = AcmeQueueWorker.from_config("worker")
        worker = ctx.resolve(binding)
        assert isinstance(worker, AcmeQueueWorker)
        assert worker.queue == "orders"

    def test_resolve_overrides_win(self, ctx: ConfigContext) -> None:
        binding = AcmeQueueWorker.from_config("worker", concurrency=1)
        worker = ctx.resolve(binding)
        assert isinstance(worker, AcmeQueueWorker)
        assert worker.concurrency == 1

    def test_resolve_configure_no_path(self, ctx: ConfigContext) -> None:
        binding = AcmeQueueWorker.configure(queue="audit", concurrency=2)
        worker = ctx.resolve(binding)
        assert isinstance(worker, AcmeQueueWorker)
        assert worker.queue == "audit"
        assert worker.concurrency == 2


class TestConfigContextCustomBinder:
    def test_accepts_strict_binder(self) -> None:
        cfg = OmegaConf.create({"worker": {"queue": "q", "concurrency": "not-an-int"}})
        strict_ctx = ConfigContext(cfg, binder=StructBinder(strict=True))
        with pytest.raises(ConfigError):
            strict_ctx.bind(AcmeQueueWorker, path="worker")


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

    def test_a_key_set_to_null_is_none_not_the_default(self) -> None:
        ctx = ConfigContext.from_dict({"rest": {"docs_url": None}})

        assert (ctx.raw("rest.docs_url", "absent"), ctx.raw("rest.redoc_url", "absent")) == (
            None,
            "absent",
        )


class TestConfigContextPositiveSeconds:
    @pytest.mark.parametrize(
        ("config", "seconds"),
        [({"acme": {"timeout": 5}}, 5.0), ({}, 30.0)],
        ids=["configured", "default-when-absent"],
    )
    def test_reads_the_seconds_or_the_default(
        self, config: dict[str, object], seconds: float
    ) -> None:
        assert ConfigContext.from_dict(config).positive_seconds("acme.timeout", 30.0) == seconds

    @pytest.mark.parametrize(
        "seconds", [0, -1, math.inf, math.nan], ids=["zero", "negative", "infinite", "nan"]
    )
    @pytest.mark.parametrize(
        "key", ["identity.max_delegation_seconds", "stores.settings.rollback_timeout"]
    )
    def test_refuses_anything_but_a_finite_number_above_zero(
        self, key: str, seconds: float
    ) -> None:
        *parents, name = key.split(".")
        raw: dict[str, Any] = {name: seconds}
        for parent in reversed(parents):
            raw = {parent: raw}
        ctx = ConfigContext.from_dict(raw)

        with pytest.raises(ConfigError, match=rf"{key} must be a finite number above zero"):
            ctx.positive_seconds(key, 30.0)
