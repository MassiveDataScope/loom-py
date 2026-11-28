"""Unit tests for StructBinder."""

from __future__ import annotations

from typing import Literal

import pytest

from loom.core.config import ConfigError, StructBinder
from loom.core.model import LoomFrozenStruct
from tests.unit.core.config.doubles import AcmeDbConfig


class _LimitsConfig(LoomFrozenStruct, frozen=True):
    max_connections: int
    max_keepalive: int


class _ClientConfig(LoomFrozenStruct, frozen=True):
    timeout_s: float
    limits: _LimitsConfig


class _PrimitiveTarget:
    def __init__(self, *, host: str, port: int = 8080) -> None:
        self.host = host
        self.port = port


class _StructTarget:
    def __init__(self, db: AcmeDbConfig, label: str = "default") -> None:
        self.db = db
        self.label = label


class _LiteralTarget:
    def __init__(self, mode: Literal["fast", "slow"], retries: int = 3) -> None:
        self.mode = mode
        self.retries = retries


class _NestedStructTarget:
    def __init__(self, client: _ClientConfig) -> None:
        self.client = client


class _MixedTarget:
    def __init__(
        self,
        db: AcmeDbConfig,
        mode: Literal["batch", "stream"],
        batch_size: int = 1000,
        label: str = "orders",
    ) -> None:
        self.db = db
        self.mode = mode
        self.batch_size = batch_size
        self.label = label


@pytest.fixture
def binder() -> StructBinder:
    return StructBinder()


class TestStructBinderPrimitives:
    def test_converts_required_primitive(self, binder: StructBinder) -> None:
        obj = binder.convert(_PrimitiveTarget, {"host": "localhost"})
        assert obj.host == "localhost"
        assert obj.port == 8080

    def test_converts_optional_primitive_override(self, binder: StructBinder) -> None:
        obj = binder.convert(_PrimitiveTarget, {"host": "db", "port": 5432})
        assert obj.port == 5432

    def test_raises_config_error_for_missing_required(self, binder: StructBinder) -> None:
        with pytest.raises(ConfigError, match="requires config field 'host'"):
            binder.convert(_PrimitiveTarget, {})

    def test_coerces_string_to_int_in_non_strict_mode(self, binder: StructBinder) -> None:
        obj = binder.convert(_PrimitiveTarget, {"host": "h", "port": "9000"})
        assert obj.port == 9000

    def test_strict_mode_rejects_string_for_int(self) -> None:
        strict = StructBinder(strict=True)
        with pytest.raises(ConfigError, match="_PrimitiveTarget.port"):
            strict.convert(_PrimitiveTarget, {"host": "h", "port": "9000"})


class TestStructBinderStructs:
    def test_converts_dict_to_struct(self, binder: StructBinder) -> None:
        obj = binder.convert(_StructTarget, {"db": {"host": "pg", "port": 5432}})
        assert isinstance(obj.db, AcmeDbConfig)
        assert obj.db.host == "pg"

    def test_struct_with_default_field(self, binder: StructBinder) -> None:
        obj = binder.convert(_StructTarget, {"db": {"host": "pg"}})
        assert obj.db.port == 5432

    def test_raises_config_error_for_invalid_struct_field(self, binder: StructBinder) -> None:
        with pytest.raises(ConfigError, match="_StructTarget.db"):
            binder.convert(_StructTarget, {"db": {"host": "pg", "port": "wrong"}})

    def test_nested_struct_conversion(self, binder: StructBinder) -> None:
        raw = {
            "client": {
                "timeout_s": 5.0,
                "limits": {"max_connections": 50, "max_keepalive": 20},
            }
        }
        obj = binder.convert(_NestedStructTarget, raw)
        assert isinstance(obj.client, _ClientConfig)
        assert isinstance(obj.client.limits, _LimitsConfig)
        assert obj.client.limits.max_connections == 50


class TestStructBinderLiteral:
    def test_accepts_valid_literal(self, binder: StructBinder) -> None:
        obj = binder.convert(_LiteralTarget, {"mode": "fast"})
        assert obj.mode == "fast"

    def test_raises_config_error_for_invalid_literal(self, binder: StructBinder) -> None:
        with pytest.raises(ConfigError, match="_LiteralTarget.mode"):
            binder.convert(_LiteralTarget, {"mode": "turbo"})


class TestStructBinderMixed:
    def test_converts_struct_literal_and_primitive_together(self, binder: StructBinder) -> None:
        raw = {
            "db": {"host": "pg", "port": 5432},
            "mode": "batch",
            "batch_size": 500,
        }
        obj = binder.convert(_MixedTarget, raw)
        assert isinstance(obj.db, AcmeDbConfig)
        assert obj.mode == "batch"
        assert obj.batch_size == 500
        assert obj.label == "orders"

    def test_extra_keys_in_raw_are_passed_through(self, binder: StructBinder) -> None:
        raw = {"host": "h", "port": 80, "unknown_key": "ignored_by_target"}
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            binder.convert(_PrimitiveTarget, raw)
