"""The A3 probe app (plan 6.8): a marker use case and an agent use case, deployable as is.

Each invocation records ``time.time()`` and ``time.monotonic()`` and logs one
record -- also its HTTP answer -- with:

* ``wall_gap`` and ``mono_gap``: seconds since the previous invocation on each
  clock (``None`` on the first). After a thaw the wall clock has moved and the
  monotonic clock, if it stood still during the freeze, has not;
* ``connects``: TCP connects opened since the previous record (the first record
  includes Init's), counted by wrapping httpcore2's network backend;
* ``mcp_ms``: the MCP call (marker) or the whole agent run (agent), in ms;
* ``init_mcp_ms`` (first record only): from Init's first TCP connect to the
  end of its ``tools/list``, the MCP part of a cold Init.

Production code reads no monotonic clock and wraps nothing: this module does,
and only once :func:`install` (deployed) or a test (through :func:`patches`)
applies its patches. The agent's model is scripted: it calls the tool once with
the arguments the prompt carries, then answers, so the run measures MCP only.

It starts as a production application: no ``LOOM_ENV`` is needed, and every
deploy check passes on the configuration it writes. Deploying: package this
module with loom, set the handler to
``<module>.lambda_handler`` (``tests.integration.ai.freeze.probe.lambda_handler``
when the ``tests`` package is shipped), 1024 MB, and the environment:

* ``A3_MCP_URL``: the gateway's MCP URL (required);
* ``A3_MCP_TOOL``: a tool that answers cheaply (default ``echo``);
* ``A3_MCP_AUTH``: the server's ``auth`` block as JSON (optional);
* ``A3_JWT_SECRET``: the HS256 secret callers sign their bearer token with
  (audience ``a3-probe``, issuer ``a3-harness``; an agent refuses an anonymous
  caller) (required);
* ``A3_CURSOR_KEY``: the cursor signing key, base64 of at least 32 bytes
  (``openssl rand -base64 32``) (required; the probe pages nothing, but a
  deployed application signs its cursors);
* ``A3_DIRECTORY``: where the configuration is written (default ``/tmp/a3``).

The routes are ``POST /a3/marker`` and ``POST /a3/agent``, with a body
``{"arguments": {...}}`` handed to the tool, and a bearer token.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, Final

import yaml
from httpcore2._backends.auto import AutoBackend
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel

from loom.ai import registry
from loom.ai.abc import AgentHandle, McpHandle
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider, _mcp
from loom.core.command import Command
from loom.core.use_case import Agent, Input, Mcp, UseCase
from loom.rest.model import RestInterface, RestRoute

SERVER: Final = "a3"
AGENT: Final = "a3_probe"
TOOL: Final = os.environ.get("A3_MCP_TOOL", "echo")
AUDIENCE: Final = "a3-probe"
"""The audience of the probe's JWTs; agents refuse an anonymous caller."""
ISSUER: Final = "a3-harness"
"""The issuer of the probe's JWTs; a deployed ``jwt`` section must name one."""
CURSOR_KEY_VARIABLE: Final = "A3_CURSOR_KEY"
"""The environment variable the cursor signing key is read from (base64, >= 32 bytes)."""
RECORD_PREFIX: Final = "A3_PROBE "
"""Starts every record's log line, followed by the record as JSON."""


@dataclass
class Recorder:
    """What one process saw: the last invocation's clocks, connects, and Init's MCP window."""

    connects: int = 0
    invocations: int = 0
    _reported: int = 0
    _wall: float | None = None
    _mono: float | None = None
    _first_connect: float | None = None
    _listed: float | None = None

    def connected(self) -> None:
        if self._first_connect is None:
            self._first_connect = perf_counter()
        self.connects += 1

    def listed(self) -> None:
        if self.invocations == 0:
            self._listed = perf_counter()

    def begin(self) -> tuple[float | None, float | None]:
        """Stamp both clocks; the gaps since the previous invocation."""
        wall, mono = time.time(), time.monotonic()
        gaps = (
            None if self._wall is None else wall - self._wall,
            None if self._mono is None else mono - self._mono,
        )
        self._wall, self._mono = wall, mono
        self.invocations += 1
        return gaps

    def emit(
        self, path: str, gaps: tuple[float | None, float | None], started: float
    ) -> dict[str, Any]:
        """Log this invocation's record and return it."""
        elapsed = (perf_counter() - started) * 1000
        init = None
        if self.invocations == 1 and None not in (self._first_connect, self._listed):
            init = (self._listed - self._first_connect) * 1000  # type: ignore[operator]
        record = {
            "probe": path,
            "invocation": self.invocations,
            "wall_gap": gaps[0],
            "mono_gap": gaps[1],
            "connects": self.connects - self._reported,
            "mcp_ms": elapsed,
            "init_mcp_ms": init,
        }
        self._reported = self.connects
        print(RECORD_PREFIX + json.dumps(record), flush=True)
        return record


RECORDER = Recorder()


class Call(Command, frozen=True):
    """The tool's arguments."""

    arguments: dict[str, Any] = {}


class ProbeMarker(UseCase[object, dict[str, Any]]):
    """Calls the tool once through an ``Mcp()`` marker."""

    async def execute(
        self, cmd: Call = Input(), server: McpHandle = Mcp(SERVER, include=(TOOL,))
    ) -> dict[str, Any]:
        gaps = RECORDER.begin()
        started = perf_counter()
        await server.call_untyped(TOOL, cmd.arguments)
        return RECORDER.emit("marker", gaps, started)


class ProbeAgent(UseCase[object, dict[str, Any]]):
    """Runs the scripted agent, which calls the tool once."""

    async def execute(
        self, cmd: Call = Input(), agent: AgentHandle = Agent(AGENT)
    ) -> dict[str, Any]:
        gaps = RECORDER.begin()
        started = perf_counter()
        await agent.run(json.dumps(cmd.arguments))
        return RECORDER.emit("agent", gaps, started)


class ProbeInterface(RestInterface[object]):
    prefix = "/a3"
    routes = (
        RestRoute(use_case=ProbeMarker, method="POST", path="/marker"),
        RestRoute(use_case=ProbeAgent, method="POST", path="/agent"),
    )


USE_CASES = [ProbeMarker, ProbeAgent]
INTERFACES = [ProbeInterface]
AGENTS = ["agents/*.yaml"]


# ---------------------------------------------------------------------------
# The scripted model and the patches
# ---------------------------------------------------------------------------


def _next(messages: list[ModelMessage], info: AgentInfo) -> tuple[str, str]:
    """Call the tool with the prompt's arguments; once it returned, answer."""
    returned = [part for part in messages[-1].parts if isinstance(part, ToolReturnPart)]
    if returned:
        return info.output_tools[0].name, json.dumps({"answer": returned[0].model_response_str()})
    prompt = messages[0].parts[-1]
    return TOOL, str(getattr(prompt, "content", "{}"))


def _respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    name, arguments = _next(messages, info)
    return ModelResponse(parts=[ToolCallPart(tool_name=name, args=arguments)])


async def _stream(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[DeltaToolCalls]:
    name, arguments = _next(messages, info)
    yield {0: DeltaToolCall(name=name, json_args=arguments, tool_call_id=f"a3-{len(messages)}")}


_PROVIDER = PydanticAIEngineProvider(
    model_resolver=lambda target: FunctionModel(_respond, stream_function=_stream)
)


def patches() -> list[tuple[object, str, object]]:
    """``(target, attribute, value)`` for each patch the probe needs, not yet applied."""
    connect_tcp = AutoBackend.connect_tcp
    list_tools = _mcp._ToolsetSession.list_tools

    async def counted_connect(self: AutoBackend, *args: Any, **kwargs: Any) -> Any:
        RECORDER.connected()
        return await connect_tcp(self, *args, **kwargs)

    async def timed_listing(self: Any) -> Any:
        listed = await list_tools(self)
        RECORDER.listed()
        return listed

    for wrapper in (counted_connect, timed_listing):
        wrapper.a3_probe = True  # type: ignore[attr-defined]
    return [
        (AutoBackend, "connect_tcp", counted_connect),
        (_mcp._ToolsetSession, "list_tools", timed_listing),
        (registry, "resolve_engine_provider", lambda name: _PROVIDER),
    ]


def install() -> None:
    """Apply every patch for the life of the process (the deployed function)."""
    for target, name, value in patches():
        setattr(target, name, value)


def project(
    directory: Path, url: str, *, secret: str, auth: Mapping[str, Any] | None = None
) -> Path:
    """Write the probe's configuration and agent under *directory*; the configuration's path.

    Args:
        directory: Where to write; the application's code path.
        url: The MCP server's URL.
        secret: The HS256 secret bearer tokens are signed with.
        auth: The MCP server's ``auth`` block, if it needs one.
    """
    key = directory / "jwt.key"
    key.parent.mkdir(parents=True, exist_ok=True)
    key.write_text(secret, encoding="utf-8")
    agents = directory / "agents"
    agents.mkdir(parents=True, exist_ok=True)
    spec = {
        "spec_version": 1,
        "name": AGENT,
        "description": "Calls the A3 tool once.",
        "instructions": "Call the tool.",
        "output": {
            "kind": "json_schema",
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "required": ["answer"],
                "properties": {"answer": {"type": "string"}},
            },
        },
        "capabilities": [{"kind": "mcp", "server": SERVER, "include": [TOOL]}],
    }
    (agents / f"{AGENT}.yaml").write_text(yaml.safe_dump(spec), encoding="utf-8")
    server: dict[str, Any] = {"url": url, "timeout_ms": 5000}
    if auth is not None:
        server["auth"] = dict(auth)
    config = {
        "app": {
            "name": "a3-probe",
            "code_path": str(directory),
            "discovery": {"mode": "manifest", "manifest": {"module": __name__}},
            "rest": {
                "auth": {
                    "jwt": {
                        "secret_path": str(key),
                        "algorithms": ["HS256"],
                        "audience": AUDIENCE,
                        "issuer": ISSUER,
                    }
                }
            },
        },
        "ai": {
            "engine": "pydantic-ai",
            "models": {"default": {"provider": "fake", "model": "fake-model"}},
            "mcp_servers": {SERVER: server},
        },
        # Unused by the probe, but an application declares a store; a deployed one signs
        # its cursors with a configured key (loom.cursors.E001).
        "stores": {"default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"}},
        "persistence": {
            "cursors": {"current_key_id": "a3", "keys": {"a3": {"env": CURSOR_KEY_VARIABLE}}}
        },
    }
    path = directory / "app.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return path


_HANDLER: list[Any] = []


def __getattr__(name: str) -> Any:
    """Build ``lambda_handler`` when the Lambda runtime resolves it, during Init."""
    if name != "lambda_handler":
        raise AttributeError(name)
    if not _HANDLER:
        from loom.composition import create_app
        from loom.serverless.aws.lambda_ import LambdaAdapter

        install()
        auth = json.loads(os.environ.get("A3_MCP_AUTH", "null"))
        directory = Path(os.environ.get("A3_DIRECTORY", "/tmp/a3"))  # noqa: S108
        url, secret = os.environ["A3_MCP_URL"], os.environ["A3_JWT_SECRET"]
        config = project(directory, url, secret=secret, auth=auth)
        _HANDLER.append(create_app(str(config), adapter=LambdaAdapter()))
    return _HANDLER[0]
