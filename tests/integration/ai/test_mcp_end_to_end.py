"""An ``mcp`` grant against a **real** MCP server, compiler to tool result.

Every other MCP test in the suite substitutes something: ``test_runtime.py``
hands the runtime a recording session, and ``test_grant_wiring.py`` replaces the
engine-side toolset (``_capabilities._mcp_server``) with an empty
``FunctionToolset``. Both therefore prove the *wiring* and none of them proves
the *protocol*: nobody has ever checked that a real
:class:`pydantic_ai.mcp.MCPToolset` speaks to a real server, that the declared
``include`` is applied to the tools that server genuinely advertises, or that a
tool result travelling back through the engine is summarised rather than
relayed.

This module closes that hole. The server is a ``fastmcp`` process-local
application exposing two deliberately distinguishable tools — ``read_orders``
and ``write_orders`` — so an ``include: [read_*]`` has something real to select
and something real to drop.

**Transport: Streamable HTTP over TLS on an ephemeral loopback port.** The
transport is not a free choice. :class:`~loom.ai.config.AiConfig` refuses any
``ai.mcp_servers.<name>.url`` that is not ``https://`` (``MCP_URL_INVALID``),
so a plaintext ``http://127.0.0.1:…`` server could not be *declared* — the
deployment configuration would be rejected before the compiler ever ran, and
the test would prove nothing about MCP. The server therefore serves real TLS
from a self-signed certificate generated per session, and the client is pointed
at it by exporting ``SSL_CERT_FILE``: httpx builds its default SSL context from
that variable, which is the only trust anchor available here because
:func:`~loom.ai.engines.pydantic_ai._mcp.build_mcp_toolset` constructs
``MCPToolset(url)`` and exposes no ``verify`` seam. Port ``0`` is bound by the
fixture so concurrent runs never collide.

The server runs in its own thread with its own event loop: each test gets a
fresh loop from pytest-asyncio, and a session-scoped server must outlive all of
them.
"""

from __future__ import annotations

import asyncio
import datetime
import os
import socket
import threading
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import msgspec
import pytest
import uvicorn
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from loom.ai.abc import AgentEvent, ToolResultEvent
from loom.ai.compiler import AgentCompiler, AgentPlan
from loom.ai.config import AiConfig, McpServerConfig
from loom.ai.declarative import AgentSpecV1, JsonSchemaOutput, McpCapability
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.model.redaction import Dsn
from loom.core.use_case.registry import UseCaseRegistry
from tests.integration.ai.conftest import CANARY, CapabilityDepsFactory, ScriptedToolModel

# fastmcp lives in the optional 'mcp-tests' group, not in 'dev': it pulls
# ~26 packages and resolves py-key-value-aio backwards, a transitive shared
# with prefect. Skipping at collection keeps the default suite green for a
# contributor who never asked for it — 'uv sync --group mcp-tests' to run these.
FastMCP = pytest.importorskip(
    "fastmcp", reason="fastmcp is not installed: uv sync --group mcp-tests"
).FastMCP

_AGENT = "order-clerk"
_SERVER = "orders"
_PROMPT = "What has acme ordered?"


_ANSWER_SCHEMA: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["answer"],
    "properties": {"answer": {"type": "string"}},
}


# ---------------------------------------------------------------------------
# The real MCP server
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LiveMcpServer:
    """A running MCP server and the trust anchor a client needs to reach it.

    Attributes:
        url: ``https://`` Streamable HTTP endpoint of the server.
        ca_path: PEM file the client must trust to verify the server.
    """

    url: str
    ca_path: Path


def _write_self_signed(directory: Path) -> tuple[Path, Path]:
    """Issue a short-lived self-signed certificate for ``localhost``."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(hours=2))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    certificate_path = directory / "server.pem"
    key_path = directory / "server.key"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return certificate_path, key_path


# ``FastMCP`` comes from ``pytest.importorskip``, so pyright sees a variable where a
# type is written; pre-existing: the AI test tree is not under pyright (task A7).
def _order_server() -> FastMCP[None]:  # pyright: ignore[reportInvalidTypeForm]
    """Build the MCP application exposing one readable and one writable tool."""
    server: FastMCP[None] = FastMCP("orders")  # pyright: ignore[reportInvalidTypeForm]

    @server.tool
    def read_orders(customer: str) -> dict[str, Any]:
        """Return the orders of one customer."""
        return {"customer": customer, "marker": CANARY, "orders": [17, 23, 41]}

    @server.tool
    def write_orders(customer: str, amount: int) -> str:
        """Record one order for a customer."""
        return f"recorded {amount} for {customer}"

    return server


@pytest.fixture(scope="session")
def mcp_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[LiveMcpServer]:
    """Serve the real MCP application over TLS on an ephemeral loopback port."""
    directory = tmp_path_factory.mktemp("mcp-tls")
    certificate_path, key_path = _write_self_signed(directory)
    bound = socket.socket()
    bound.bind(("127.0.0.1", 0))
    port = int(bound.getsockname()[1])
    config = uvicorn.Config(
        _order_server().http_app(path="/mcp"),
        log_level="warning",
        ssl_certfile=str(certificate_path),
        ssl_keyfile=str(key_path),
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(
        target=lambda: asyncio.run(server.serve(sockets=[bound])),
        name="loom-test-mcp-server",
        daemon=True,
    )
    thread.start()
    deadline = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=20)
    while not server.started:
        if datetime.datetime.now(datetime.UTC) > deadline:
            raise AssertionError("the test MCP server did not start inside its budget")
        threading.Event().wait(0.02)
    try:
        yield LiveMcpServer(url=f"https://localhost:{port}/mcp", ca_path=certificate_path)
    finally:
        server.should_exit = True
        thread.join(timeout=20)


@pytest.fixture
def server_url(mcp_server: LiveMcpServer, monkeypatch: pytest.MonkeyPatch) -> str:
    """Trust the server's certificate for this test, and return its URL.

    ``build_mcp_toolset`` builds ``MCPToolset(url)`` with no ``verify``
    argument, so the trust anchor can only be supplied the way httpx reads
    one: from ``SSL_CERT_FILE`` at client construction.
    """
    monkeypatch.setenv("SSL_CERT_FILE", str(mcp_server.ca_path))
    return mcp_server.url


# ---------------------------------------------------------------------------
# Real plan, real runtime
# ---------------------------------------------------------------------------


def _config(url: str) -> AiConfig:
    """Deployment configuration registering the live server under its name."""
    return AiConfig(
        engine="pydantic-ai",
        specs=(),
        models={"default": InferenceTarget(provider="fake", model="fake-model")},
        mcp_servers={_SERVER: McpServerConfig(url=Dsn(url))},
        startup_timeout_ms=20000,
        health_cache_ttl_ms=5000,
    )


def _compile(url: str, capability: McpCapability) -> AgentPlan:
    """Compile one artifact through the real compiler and the real provider kinds."""
    spec = AgentSpecV1(
        spec_version=1,
        name=_AGENT,
        description="Reads the order server on behalf of the caller.",
        instructions="Answer using only the granted tools.",
        output=JsonSchemaOutput(schema=dict(_ANSWER_SCHEMA)),
        capabilities=(capability,),
    )
    compiler = AgentCompiler(
        config=_config(url),
        registry=UseCaseRegistry.build([]),
        supported_kinds=PydanticAIEngineProvider().supported_capability_kinds(),
    )
    return compiler.compile(spec, source_path=f"ai/agents/{_AGENT}/agent.yaml")


def _runtime(url: str, plan: AgentPlan, model: ScriptedToolModel) -> Any:
    """Build the runtime the composition root builds, with the real MCP factory."""
    from loom.ai.runtime import AgentRuntime

    provider = PydanticAIEngineProvider(model_resolver=lambda target: model.as_model())
    return AgentRuntime(
        plans=[plan],
        config=_config(url),
        engine_provider=provider,
        deps=CapabilityDepsFactory(),  # type: ignore[arg-type]
        container=LoomContainer(),
        # Read off the provider, as ``create_app`` does: the start-up session
        # and the run's toolset are then one shared connection per server.
        mcp_client_factory=provider.mcp_client_factory,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFilterAgainstARealServer:
    """FR-025: ``include`` is applied to the tools the server really advertises."""

    async def test_only_the_included_tool_reaches_the_agent_when_the_grant_filters(
        self, server_url: str, caller: Identity
    ) -> None:
        """``include: [read_*]`` leaves ``write_orders`` unreachable by the model."""
        model = ScriptedToolModel(calls=(("read_orders", {"customer": "acme"}),))
        plan = _compile(server_url, McpCapability(server=_SERVER, include=("read_*",)))

        async with _runtime(server_url, plan, model) as runtime:
            await runtime.run(_AGENT, _PROMPT, identity=caller)

        assert model.offered_tools == ("read_orders",)

    async def test_the_agent_sees_both_tools_when_the_grant_does_not_filter(
        self, server_url: str, caller: Identity
    ) -> None:
        """Without a filter the whole advertised surface reaches the model."""
        model = ScriptedToolModel()
        plan = _compile(server_url, McpCapability(server=_SERVER))

        async with _runtime(server_url, plan, model) as runtime:
            await runtime.run(_AGENT, _PROMPT, identity=caller)

        assert sorted(model.offered_tools) == ["read_orders", "write_orders"]

    async def test_startup_fails_with_an_empty_filter_when_include_matches_nothing(
        self, server_url: str, caller: Identity
    ) -> None:
        """A filter selecting none of the *real* tools aborts start-up."""
        del caller
        model = ScriptedToolModel()
        plan = _compile(server_url, McpCapability(server=_SERVER, include=("purge_*",)))

        with pytest.raises(AgentCompilationError) as failure:
            async with _runtime(server_url, plan, model):
                pass

        assert AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING in {
            issue.code for issue in failure.value.issues
        }


class TestEndToEndCall:
    """A granted tool is invoked for real, and its result is summarised (FR-030b)."""

    @pytest.fixture
    async def tool_events(
        self, server_url: str, caller: Identity
    ) -> tuple[ScriptedToolModel, tuple[AgentEvent, ...]]:
        """Run one agent that calls ``read_orders``, collecting every event."""
        model = ScriptedToolModel(calls=(("read_orders", {"customer": "acme"}),))
        plan = _compile(server_url, McpCapability(server=_SERVER, include=("read_*",)))
        collected: list[AgentEvent] = []
        async with (
            _runtime(server_url, plan, model) as runtime,
            runtime.run_stream(_AGENT, _PROMPT, identity=caller) as stream,
        ):
            async for event in stream:
                collected.append(event)
        return model, tuple(collected)

    def _result(self, events: Sequence[AgentEvent]) -> ToolResultEvent:
        results = [event for event in events if isinstance(event, ToolResultEvent)]
        assert len(results) == 1, f"expected exactly one tool result, got {results}"
        return results[0]

    async def test_the_real_tool_returns_the_payload_to_the_model_when_invoked(
        self, tool_events: tuple[ScriptedToolModel, tuple[AgentEvent, ...]]
    ) -> None:
        """The call reached the server: the model was shown the served payload."""
        model, _ = tool_events

        assert any(CANARY in shown for shown in model.tool_returns)

    async def test_the_tool_result_carries_looms_summary_when_the_tool_responds(
        self, tool_events: tuple[ScriptedToolModel, tuple[AgentEvent, ...]]
    ) -> None:
        """A foreign toolset publishes no facts, so the summary degrades to ``ok``."""
        _, events = tool_events

        assert self._result(events).summary == "ok"

    async def test_the_tool_result_carries_no_payload_bytes_when_the_tool_responds(
        self, tool_events: tuple[ScriptedToolModel, tuple[AgentEvent, ...]]
    ) -> None:
        """FR-030b: no byte of the tool payload travels in the stream event."""
        _, events = tool_events

        assert CANARY not in msgspec.json.encode(self._result(events)).decode()


# ---------------------------------------------------------------------------
# Deployed server (opt-in)
# ---------------------------------------------------------------------------

LIVE_URL_VAR = "LOOM_LIVE_MCP_URL"
"""Environment variable naming a deployed MCP server to exercise instead."""


@pytest.mark.live
class TestDeployedServer:
    """The same path against a server this suite did not start.

    Opt-in and self-skipping, the pattern ``test_live_provider.py`` uses: point
    it at a deployment and run it deliberately::

        export LOOM_LIVE_MCP_URL=https://tools.example.com/mcp
        uv run pytest tests/integration/ai/test_mcp_end_to_end.py -m live

    A process-local server proves the protocol; only a deployed one proves the
    deployment's own TLS chain, its authentication front door and the tool
    surface it really publishes.
    """

    @pytest.fixture
    def live_url(self) -> str:
        """The deployed server's URL, or a skip when none is configured."""
        url = os.environ.get(LIVE_URL_VAR)
        if not url:
            pytest.skip(f"{LIVE_URL_VAR} is not set: no deployed MCP server to reach")
        return url

    async def test_the_deployed_server_publishes_some_tool_when_configured(
        self, live_url: str
    ) -> None:
        """The real client factory connects and the server advertises a surface."""
        plan = _compile(live_url, McpCapability(server=_SERVER))
        capability = plan.capabilities[0]

        async with SharedMcpToolsets().open(capability) as session:  # type: ignore[arg-type]
            tools = await session.list_tools()

        assert tools

    async def test_the_runtime_starts_when_the_deployed_server_responds(
        self, live_url: str
    ) -> None:
        """Start-up validates the grant against the deployment, not a stub."""
        plan = _compile(live_url, McpCapability(server=_SERVER))

        async with _runtime(live_url, plan, ScriptedToolModel()) as runtime:
            assert runtime.capability_kinds(_AGENT) == ("mcp",)
