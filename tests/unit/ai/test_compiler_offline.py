"""Offline guarantee for ``AgentCompiler`` (US1, T049, FR-010/SC-006).

Compiling the full valid corpus must succeed with every provider credential
unset and every socket primitive replaced by a guard that raises: touching
the network is a test failure, never a slow test.
"""

from __future__ import annotations

import socket
from pathlib import Path
from typing import NoReturn

import pytest

from loom.ai.compiler import AgentCompiler
from loom.ai.config import AiConfig
from loom.ai.declarative import load_specs
from loom.core.stores.sql.config import SqlConfig
from loom.core.use_case.registry import UseCaseRegistry
from tests.unit.ai.phases.conftest import ALL_KINDS, admits_every_native_tool

from .conftest import CORPUS_PATTERN

CORPUS_DIR = Path(__file__).parent / "fixtures" / "corpus_v1"


_PROVIDER_ENV_VARS: tuple[str, ...] = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_DEFAULT_REGION",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
)


def _network_forbidden(*args: object, **kwargs: object) -> NoReturn:
    raise AssertionError("compilation attempted network access; it must be fully offline")


@pytest.fixture(autouse=True)
def _offline_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Block socket creation and strip every provider credential."""
    monkeypatch.setattr(socket, "socket", _network_forbidden)
    monkeypatch.setattr(socket, "socketpair", _network_forbidden)
    monkeypatch.setattr(socket, "create_connection", _network_forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", _network_forbidden)
    for variable in _PROVIDER_ENV_VARS:
        monkeypatch.delenv(variable, raising=False)


class TestOfflineCompilation:
    def test_corpus_compiles_clean_when_offline_and_credentialless(
        self,
        compiler_env_config: AiConfig,
        compiler_env_registry: UseCaseRegistry,
        compiler_env_sql: SqlConfig,
        fake_myapp_path: object,
    ) -> None:
        compiler = AgentCompiler(
            config=compiler_env_config,
            registry=compiler_env_registry,
            supported_kinds=ALL_KINDS,
            sql=compiler_env_sql,
            native_tools=admits_every_native_tool,
        )
        decoded = load_specs([CORPUS_PATTERN], root=CORPUS_DIR)
        plans = compiler.compile_all(decoded)
        assert len(plans) == len(decoded) == 10
