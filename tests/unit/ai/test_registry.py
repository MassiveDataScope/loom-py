"""Engine and provider resolution contract for ``loom.ai.registry`` (T030).

Pins the deployment-resolution failures of the error-codes contract:
``ENGINE_NOT_FOUND``, ``ENGINE_DUPLICATE``, ``ENGINE_API_MISMATCH``,
``PROVIDER_NOT_INSTALLED`` and ``PROVIDER_SETTING_MISSING``. All of them must
surface as ``AgentCompilationError`` carrying the corresponding code.

Entry points are simulated by replacing ``entry_points`` inside
``loom.core.plugins.entrypoints``, mirroring
``tests/unit/core/plugins/test_entrypoints.py``.
"""

from __future__ import annotations

import pytest

from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.registry import (
    configure_engine_mcp_connect_timeout,
    require_provider_sdk,
    require_provider_setting,
    resolve_engine_provider,
)
from loom.core.plugins import entrypoints as entrypoints_module

_GROUP = "loom.ai.engines"
_ENGINE_NAME = "pydantic-ai"


class _FakeDist:
    """Minimal stand-in for ``importlib.metadata.Distribution``."""

    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEntryPoint:
    """Minimal stand-in for ``importlib.metadata.EntryPoint``."""

    def __init__(self, name: str, dist_name: str, target: object) -> None:
        self.name = name
        self.group = _GROUP
        self.dist = _FakeDist(dist_name)
        self._target = target

    def load(self) -> object:
        return self._target


class _FakeEntryPoints:
    """Stand-in for the collection returned by ``entry_points()``."""

    def __init__(self, entries: tuple[_FakeEntryPoint, ...]) -> None:
        self._entries = entries

    def select(self, *, group: str) -> tuple[_FakeEntryPoint, ...]:
        return tuple(entry for entry in self._entries if entry.group == group)


class _ProviderWithoutHandshake:
    """Loaded object lacking the ``LOOM_AI_ENGINE_API`` attribute."""


class _ProviderWithUnsupportedHandshake:
    """Loaded object announcing a handshake version this release cannot speak."""

    LOOM_AI_ENGINE_API = 99


class _ProviderDeclaringOnTheInstance:
    """Engine class declaring the handshake only once it is constructed."""

    def __init__(self) -> None:
        self.LOOM_AI_ENGINE_API = 2

    def supported_capability_kinds(self) -> frozenset[str]:
        return frozenset({"mcp"})


def _install(
    monkeypatch: pytest.MonkeyPatch,
    entries: tuple[_FakeEntryPoint, ...],
) -> None:
    """Replace ``entry_points`` inside the shared loader with a fake collection."""
    monkeypatch.setattr(
        entrypoints_module,
        "entry_points",
        lambda: _FakeEntryPoints(entries),
    )


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    """Extract the ordered issue codes carried by a compilation error."""
    return [issue.code for issue in error.issues]


class TestEngineNotFound:
    def test_fails_with_engine_not_found_when_there_are_no_entry_points(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, ())

        with pytest.raises(AgentCompilationError) as excinfo:
            resolve_engine_provider(_ENGINE_NAME)

        assert AgentErrorCode.ENGINE_NOT_FOUND in _codes(excinfo.value)

    def test_the_message_names_the_installed_engines_when_the_requested_one_is_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The failure must tell the operator which engines are installed."""
        _install(monkeypatch, (_FakeEntryPoint("other-engine", "loom-engine-beta", object()),))

        with pytest.raises(AgentCompilationError) as excinfo:
            resolve_engine_provider(_ENGINE_NAME)

        assert "other-engine" in str(excinfo.value)


class TestHandshakeOnTheInstance:
    def test_resolves_the_engine_when_the_handshake_is_declared_only_on_the_instance(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The handshake is read from the constructed provider, not from its class."""
        _install(
            monkeypatch,
            (
                _FakeEntryPoint(
                    _ENGINE_NAME,
                    "loom-engine-alpha",
                    _ProviderDeclaringOnTheInstance,
                ),
            ),
        )

        provider = resolve_engine_provider(_ENGINE_NAME)

        assert isinstance(provider, _ProviderDeclaringOnTheInstance)


class TestEngineDuplicate:
    @pytest.fixture
    def duplicate_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> AgentCompilationError:
        """Resolve against two distributions claiming the same engine name."""
        _install(
            monkeypatch,
            (
                _FakeEntryPoint(_ENGINE_NAME, "loom-engine-alpha", object()),
                _FakeEntryPoint(_ENGINE_NAME, "loom-engine-beta", object()),
            ),
        )
        with pytest.raises(AgentCompilationError) as excinfo:
            resolve_engine_provider(_ENGINE_NAME)
        return excinfo.value

    def test_fails_with_engine_duplicate_when_two_distributions_register_it(
        self,
        duplicate_error: AgentCompilationError,
    ) -> None:
        assert AgentErrorCode.ENGINE_DUPLICATE in _codes(duplicate_error)

    def test_the_message_names_both_distributions_on_a_duplicate(
        self,
        duplicate_error: AgentCompilationError,
    ) -> None:
        message = str(duplicate_error)

        assert "loom-engine-alpha" in message
        assert "loom-engine-beta" in message


class TestEngineApiMismatch:
    def test_fails_with_engine_api_mismatch_when_the_handshake_is_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A loaded object without ``LOOM_AI_ENGINE_API`` cannot be an engine."""
        _install(
            monkeypatch,
            (_FakeEntryPoint(_ENGINE_NAME, "loom-engine-alpha", _ProviderWithoutHandshake()),),
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            resolve_engine_provider(_ENGINE_NAME)

        assert AgentErrorCode.ENGINE_API_MISMATCH in _codes(excinfo.value)

    def test_fails_with_engine_api_mismatch_when_the_version_is_unsupported(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Only handshake version 1 is supported by this release."""
        _install(
            monkeypatch,
            (
                _FakeEntryPoint(
                    _ENGINE_NAME,
                    "loom-engine-alpha",
                    _ProviderWithUnsupportedHandshake(),
                ),
            ),
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            resolve_engine_provider(_ENGINE_NAME)

        assert AgentErrorCode.ENGINE_API_MISMATCH in _codes(excinfo.value)


class TestProviderHelpers:
    def test_fails_with_provider_not_installed_when_the_sdk_is_missing(self) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            require_provider_sdk("bedrock", "loom_nonexistent_sdk_xyz", "ai-bedrock")

        assert AgentErrorCode.PROVIDER_NOT_INSTALLED in _codes(excinfo.value)

    def test_the_message_names_the_extra_when_the_sdk_is_missing(self) -> None:
        """The failure must tell the operator which extra to install."""
        with pytest.raises(AgentCompilationError) as excinfo:
            require_provider_sdk("bedrock", "loom_nonexistent_sdk_xyz", "ai-bedrock")

        assert "ai-bedrock" in str(excinfo.value)

    def test_fails_with_provider_setting_missing_when_a_setting_is_absent(self) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            require_provider_setting("bedrock", "region", None)

        assert AgentErrorCode.PROVIDER_SETTING_MISSING in _codes(excinfo.value)

    def test_the_message_names_the_missing_setting(self) -> None:
        """The failure must name the missing setting, not just the provider."""
        with pytest.raises(AgentCompilationError) as excinfo:
            require_provider_setting("bedrock", "region", None)

        assert "region" in str(excinfo.value)


class TestConfigureEngineMcpConnectTimeout:
    """FR-051: the deployment's handshake budget reaches the engine (structurally)."""

    def test_calls_the_providers_method_when_it_exists(self) -> None:
        """Read with ``getattr``, exactly as documented: called when present."""
        calls: list[float] = []

        class _Provider:
            def configure_mcp_connect_timeout(self, seconds: float) -> None:
                calls.append(seconds)

        configure_engine_mcp_connect_timeout(_Provider(), 7.0)

        assert calls == [7.0]

    def test_does_nothing_when_the_provider_declares_no_such_method(self) -> None:
        """A third-party engine without this seam is left at its own default."""

        class _ProviderWithoutTheSeam:
            pass

        configure_engine_mcp_connect_timeout(_ProviderWithoutTheSeam(), 7.0)

    def test_reaches_the_real_pydantic_ai_provider_and_its_shared_toolsets(self) -> None:
        """The real, installed engine: its own ``SharedMcpToolsets`` reads the value back."""
        pytest.importorskip(
            "pydantic_ai.mcp", reason="the MCP client is not installed: uv sync --group mcp-tests"
        )
        from loom.ai.compiler import CompiledMcpCapability
        from loom.ai.engines.pydantic_ai.provider import PydanticAIEngineProvider

        provider = resolve_engine_provider(_ENGINE_NAME)
        assert isinstance(provider, PydanticAIEngineProvider)

        configure_engine_mcp_connect_timeout(provider, 7.0)

        capability = CompiledMcpCapability(server="orders", url="https://orders.example.com/mcp")
        toolset = provider._mcp._toolset(capability)  # noqa: SLF001 - the seam under test
        assert toolset.client._init_timeout == 7.0
