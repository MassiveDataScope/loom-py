"""Unit tests for declarative config bindings."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigBinding, Configurable
from loom.core.model import LoomFrozenStruct


class AcmeTimeoutWorker(Configurable):
    def __init__(self, *, timeout_ms: int = 5000) -> None:
        self.timeout_ms = timeout_ms


class TestConfigurableBinding:
    def test_from_config_returns_deferred_binding(self) -> None:
        binding = AcmeTimeoutWorker.from_config("streaming.tasks.scrape_one", timeout_ms=20_000)

        assert isinstance(binding, LoomFrozenStruct)
        assert binding.target is AcmeTimeoutWorker
        assert binding.config_path == "streaming.tasks.scrape_one"
        assert binding.overrides == {"timeout_ms": 20_000}

    def test_configure_returns_binding_without_config_path(self) -> None:
        binding = AcmeTimeoutWorker.configure(timeout_ms=10_000)

        assert binding.target is AcmeTimeoutWorker
        assert binding.config_path == ""
        assert binding.overrides == {"timeout_ms": 10_000}

    def test_from_config_rejects_empty_path(self) -> None:
        with pytest.raises(ValueError, match="config_path"):
            AcmeTimeoutWorker.from_config(" ")

    def test_config_binding_rejects_blank_non_empty_path(self) -> None:
        with pytest.raises(ValueError, match="config_path"):
            ConfigBinding(target=AcmeTimeoutWorker, config_path=" ")
