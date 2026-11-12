"""The request counts of plan 6.7, row by row, on a real Lambda adapter (T07).

Each class is one row of the cost table: what an invocation sends to the MCP
server, the TCP connects it opens, and the token fetches it makes. The
function, the server and the counters are T06's
(:mod:`tests.integration.ai.freeze.test_lambda`); both triggers run where the
row applies, and both protocol eras where the row is not era-specific.

Per-era TCP connects (measured with fastmcp 4.1.6 / mcp 2.3.0): the modern era
keeps the one connection Init opened; the handshake ("legacy") era opens one
connection per ``POST``, because the server answers each with an event stream
and the client closes that connection once it read it.

The last two classes check the A3 tools: the deployed probe app
(:mod:`~tests.integration.ai.freeze.probe`), driven here over the same live
server, and the pure parts of the harness
(:mod:`~tests.integration.ai.freeze.measure`), which needs a deployed function
to run at all.
"""

from __future__ import annotations

import base64
import contextlib
import json
import time
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

# fastmcp lives in the optional 'mcp-tests' group; skip before anything that imports it.
pytest.importorskip("fastmcp", reason="fastmcp is not installed: uv sync --group mcp-tests")

import yaml  # noqa: E402
from httpcore2._backends.auto import AutoBackend  # noqa: E402

from loom.ai import registry  # noqa: E402
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider, _mcp  # noqa: E402
from loom.ai.engines.pydantic_ai import _transport as _transport  # noqa: E402
from loom.ai.errors import AgentRunErrorCode  # noqa: E402
from loom.composition import create_app  # noqa: E402
from loom.serverless.aws.lambda_ import LambdaAdapter, LambdaHandler  # noqa: E402
from loom.testing import LambdaTestClient  # noqa: E402
from tests.integration.ai.freeze import measure, probe  # noqa: E402
from tests.integration.ai.freeze.server import Era, LiveMcpServer, serving  # noqa: E402

# The T06 function and its fixtures: importing the fixtures registers them here.
from tests.integration.ai.freeze.test_lambda import (  # noqa: E402
    _AGENT,
    _ERAS,
    _HANDSHAKE,
    _PATHS,
    _POST_CONNECTS,
    _RAISED,
    _TOKEN_KIND,
    _TRIGGERS,
    FetchedToken,
    Path_,
    Trigger,
    World,
    _Model,
    _WallClock,
    _warmed,
    _World,
    _write_project,
)
from tests.integration.ai.freeze.test_lambda import _isolated as _isolated  # noqa: E402
from tests.integration.ai.freeze.test_lambda import _no_proxy as _no_proxy  # noqa: E402
from tests.integration.ai.freeze.test_lambda import world as world  # noqa: E402

pytestmark = [
    pytest.mark.timeout(60),
    pytest.mark.filterwarnings("ignore:Celery config section is missing"),
    pytest.mark.filterwarnings("ignore:app.job_service is not set"),
]

_RENEWAL = ["server/discover", "initialize", "notifications/initialized", "tools/list"]
"""What a renewal sends in the handshake era: the era probe, then the handshake and one list."""

_LEGACY_INIT_CONNECTS = 4
"""Legacy Init: ``server/discover`` and ``initialize`` share one connection (the
probe is answered with plain JSON, which leaves it pooled), then
``notifications/initialized``, the GET stream and ``tools/list`` one each."""

_LEGACY_RENEWAL_CONNECTS = 6
"""A legacy call answered 404, its renewal and the retried call: the 404 call (the
JSON answer leaves its connection pooled, and the old session's ``DELETE`` reuses
it), then Init's four, then the retried call."""


class _ClockReads:
    """Counts the wall-clock reads of every pool, over the clock the function was built with."""

    def __init__(self, clock: _WallClock) -> None:
        self._clock = clock
        self.reads = 0

    def time(self) -> float:
        self.reads += 1
        return self._clock.time()


def _posts(built: _World) -> int:
    """``POST`` requests the server logged since the mark."""
    return sum(request.verb == "POST" for request in built.server.log[built.since.log :])


# ---------------------------------------------------------------------------
# Plan 6.7, one class per row
# ---------------------------------------------------------------------------


class TestRowWarmInvocation:
    """Warm invocation (gap <= 5 s): only the tool calls; 0 TCP (modern), 0 token fetches."""

    @_PATHS
    @_TRIGGERS
    @_ERAS
    def test_sends_only_its_tool_call(
        self, world: World, era: Era, trigger: Trigger, path: Path_
    ) -> None:
        built = _warmed(world, era, trigger)
        fetched = FetchedToken.fetches

        for _ in range(5):
            built.freeze(1.0)
            assert built.by(path)

        assert built.methods() == ["tools/call"] * 5
        assert built.connects() == _POST_CONNECTS[era](5)
        assert FetchedToken.fetches - fetched == 0
        assert (built.evictions(), built.rebuilds()) == (0, 0)

    @_TRIGGERS
    def test_the_freeze_check_reads_the_wall_clock_once_per_request(
        self, world: World, trigger: Trigger, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One read to check the gap; one more stamps the clock when the body closes."""
        built = _warmed(world, "modern", trigger)
        reads = _ClockReads(built.clock)
        monkeypatch.setattr(_transport, "time", reads)

        built.freeze(1.0)
        assert built.call()

        assert _posts(built) == 1
        assert reads.reads == 2


class TestRowThawSessionStillValid:
    """First call after a thaw (gap > 5 s), session valid: only the call; 1 TCP (eviction)."""

    @_PATHS
    @_TRIGGERS
    @_ERAS
    def test_sends_only_the_call_over_one_new_connection(
        self, world: World, era: Era, trigger: Trigger, path: Path_
    ) -> None:
        built = _warmed(world, era, trigger)
        fetched = FetchedToken.fetches
        built.server.stale()
        built.freeze(6.0)

        assert built.by(path)

        assert built.methods() == ["tools/call"]
        assert built.count("initialize", "tools/list") == 0
        assert built.connects() == 1
        assert FetchedToken.fetches - fetched == 0
        assert (built.evictions(), built.rebuilds()) == (1, 0)

    @_TRIGGERS
    def test_the_legacy_event_stream_reconnects_off_the_call_path(
        self, world: World, trigger: Trigger
    ) -> None:
        """The GET stream's own reconnect is the row's background +1 TCP."""
        built = _warmed(world, "legacy", trigger)
        built.server.stale()
        built.freeze(6.0)
        assert built.call()
        assert (built.connects(), built.streams()) == (1, 1)

        time.sleep(1.1)  # the mcp client's reconnection delay, DEFAULT_RECONNECTION_DELAY_MS
        built.freeze(1.0)
        assert built.call()
        built.settle(streams=2)

        assert built.connects() == 1 + 1 + 1  # the thawed call, the stream, the next call
        assert built.methods() == ["tools/call", "tools/call"]
        assert (built.evictions(), built.rebuilds()) == (1, 0)


class TestRowThawLegacySessionExpired:
    """First call after a thaw, legacy session expired: the 404 call, a renewal, the retry."""

    @_PATHS
    @_TRIGGERS
    def test_sends_the_failed_call_a_renewal_and_the_retried_call(
        self, world: World, trigger: Trigger, path: Path_
    ) -> None:
        built = _warmed(world, "legacy", trigger)
        fetched = FetchedToken.fetches
        built.server.stale()
        built.server.forget()
        built.freeze(6.0)

        assert built.by(path)
        built.settle(streams=2)  # the new session's stream, opened within the invocation

        assert built.methods() == ["tools/call", *_RENEWAL, "tools/call"]
        assert built.connects() == _LEGACY_RENEWAL_CONNECTS
        assert built.runs()["echo"] == 1
        assert FetchedToken.fetches - fetched == 0
        assert (built.evictions(), built.rebuilds()) == (1, 1)


class TestRowExpiredSessionSafeRetry:
    """First call after an expired session, no thaw: 404 (nothing run), renewal, retried call."""

    @_PATHS
    @_TRIGGERS
    def test_the_404_attempt_is_retried_after_one_renewal(
        self, world: World, trigger: Trigger, path: Path_
    ) -> None:
        """``tools/list`` is sent on both paths: a renewal lists once for both (plan 6.6)."""
        built = _warmed(world, "legacy", trigger)
        fetched = FetchedToken.fetches
        built.server.forget()
        built.freeze(1.0)

        assert built.by(path)
        built.settle(streams=2)  # the row's "(+ the GET stream if the client opens one)"

        assert built.methods() == ["tools/call", *_RENEWAL, "tools/call"]
        assert built.count("tools/list") == 1
        assert built.streams() == 2
        assert built.connects() == _LEGACY_RENEWAL_CONNECTS
        assert built.runs()["echo"] == 1
        assert FetchedToken.fetches - fetched == 0
        assert (built.evictions(), built.rebuilds()) == (0, 1)
        assert _RAISED == []


class TestRowMidCallFailure:
    """Mid-call failure (unsafe): the failed call, 0 retries; the next call pays a renewal."""

    @_PATHS
    @_TRIGGERS
    @_ERAS
    def test_sends_the_call_once_and_the_next_call_renews(
        self, world: World, era: Era, trigger: Trigger, path: Path_
    ) -> None:
        built = _warmed(world, era, trigger)
        built.server.cut_next_call()

        assert not built.by(path, "book")

        assert built.methods() == ["tools/call"]
        assert built.runs()["book"] == 1
        assert _RAISED == [AgentRunErrorCode.MCP_CALL_INTERRUPTED]
        fetched = FetchedToken.fetches

        assert built.by(path)

        renewal = _RENEWAL if era == "legacy" else ["server/discover", "tools/list"]
        assert built.methods() == ["tools/call", *renewal, "tools/call"]
        assert built.runs() == {"book": 1, "echo": 1}
        assert FetchedToken.fetches - fetched == 0
        assert built.rebuilds() == 1


class TestRowColdInit:
    """Cold Init, per reachable server: the handshake and one list; 1 token fetch."""

    @_TRIGGERS
    @_ERAS
    def test_opens_each_reachable_server_once(
        self, world: World, era: Era, trigger: Trigger
    ) -> None:
        built = world(era, trigger)

        handshake = list(_HANDSHAKE) if era == "legacy" else ["server/discover", "tools/list"]
        assert [request.method for request in built.server.log if request.method] == handshake
        assert built.server.connects == (1 if era == "modern" else _LEGACY_INIT_CONNECTS)
        assert FetchedToken.fetches == 1
        assert len(built.builds) == 1

    @_TRIGGERS
    def test_a_server_only_an_unreachable_agent_uses_gets_no_request(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, trigger: Trigger
    ) -> None:
        with serving("modern") as server, serving("modern") as spare:
            handler = _function_with_a_spare_server(tmp_path, monkeypatch, server, spare, trigger)
            try:
                assert server.log
                assert (spare.log, spare.connects) == ([], 0)
                assert FetchedToken.fetches == 1
            finally:
                handler.close()


def _function_with_a_spare_server(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    server: LiveMcpServer,
    spare: LiveMcpServer,
    trigger: Trigger,
) -> LambdaHandler:
    """T06's function, plus an agent no use case or job reaches, granted a second server."""
    monkeypatch.setattr(_transport, "time", _WallClock())
    provider = PydanticAIEngineProvider(model_resolver=_Model().model)
    monkeypatch.setattr(registry, "resolve_engine_provider", lambda name: provider)
    path = Path(_write_project(tmp_path, server.url, trigger, agents=True))
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    spare_server = {"url": spare.url, "timeout_ms": 5000, "auth": {"kind": _TOKEN_KIND}}
    config["ai"]["mcp_servers"]["spare"] = spare_server
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    agents = tmp_path / "agents"
    spec = yaml.safe_load((agents / f"{_AGENT}.yaml").read_text(encoding="utf-8"))
    grant = {"kind": "mcp", "server": "spare", "include": ["echo"]}
    spec |= {"name": "idle", "capabilities": [grant]}
    (agents / "idle.yaml").write_text(yaml.safe_dump(spec), encoding="utf-8")
    function = "worker" if trigger == "jobs" else None
    return create_app(str(path), adapter=LambdaAdapter(function=function), environment="test")


# ---------------------------------------------------------------------------
# The deployed probe app, run locally
# ---------------------------------------------------------------------------


_PROBE_SECRET = "t07-probe-secret-of-at-least-32-bytes"
_CURSOR_KEY = base64.b64encode(b"t07-probe-cursor-key-of-32-bytes").decode()


class _ThawClock(_WallClock):
    """The pool's and the probe's clock: a wall clock a test moves; the real monotonic clock."""

    def monotonic(self) -> float:
        return time.monotonic()


@dataclass
class _ProbeFunction:
    server: LiveMcpServer
    handler: LambdaHandler
    clock: _ThawClock
    capsys: pytest.CaptureFixture[str]

    def invoke(self, path: str) -> dict[str, Any]:
        """One invocation of the probe's *path* use case; the record it answered with.

        Also checks that the record it logged is the same one.
        """
        client = LambdaTestClient(self.handler)  # type: ignore[arg-type]
        body = {"arguments": {"text": "probe"}}
        response = client.post(f"/a3/{path}", json=body, headers=measure.bearer(_PROBE_SECRET))
        assert response.status_code == 200, response.text
        record = response.json()
        logged = [
            json.loads(line.removeprefix(probe.RECORD_PREFIX))
            for line in self.capsys.readouterr().out.splitlines()
            if line.startswith(probe.RECORD_PREFIX)
        ]
        assert logged == [record]
        return record


@pytest.fixture
def probe_function(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> Iterator[Any]:
    """Builds the probe app over a live server of one era, with the probe's patches applied."""
    with contextlib.ExitStack() as stack:

        def build(era: Era) -> _ProbeFunction:
            server = stack.enter_context(serving(era))
            clock = _ThawClock()
            monkeypatch.setattr(_transport, "time", clock)
            monkeypatch.setattr(probe, "time", clock)
            monkeypatch.setattr(probe, "RECORDER", probe.Recorder())
            for target, name, value in probe.patches():
                monkeypatch.setattr(target, name, value)
            monkeypatch.setenv(probe.CURSOR_KEY_VARIABLE, _CURSOR_KEY)
            config = probe.project(tmp_path, server.url, secret=_PROBE_SECRET)
            handler = create_app(str(config), adapter=LambdaAdapter(), environment="test")
            stack.callback(handler.close)
            return _ProbeFunction(server, handler, clock, capsys)

        yield build


class TestTheProbe:
    @_ERAS
    def test_a_thawed_invocation_logs_both_gaps_and_its_connects(
        self, probe_function: Any, era: Era
    ) -> None:
        function: _ProbeFunction = probe_function(era)
        first = function.invoke("marker")
        function.server.stale()
        function.clock.now += 6.0

        record = function.invoke("marker")

        assert record["wall_gap"] > 5
        assert 0 <= record["mono_gap"] < 5
        assert record["connects"] == 1
        assert first["wall_gap"] is None and first["init_mcp_ms"] > 0

    @_PATHS
    @_ERAS
    def test_counts_the_connects_of_each_warm_call(
        self, probe_function: Any, era: Era, path: str
    ) -> None:
        """The probe's count agrees with the server's: legacy era, one per ``POST``."""
        function: _ProbeFunction = probe_function(era)
        first = function.invoke(path)
        assert first["connects"] == function.server.connects  # Init's and its own

        connects = function.server.connects
        function.clock.now += 1.0
        record = function.invoke(path)

        assert record["connects"] == _POST_CONNECTS[era](1)
        assert function.server.connects - connects == record["connects"]
        assert record["probe"] == path and record["mcp_ms"] > 0
        assert record["wall_gap"] >= 1.0 and record["init_mcp_ms"] is None


def test_the_deployed_handler_starts_in_production_from_its_documented_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``probe.lambda_handler`` as Lambda resolves it: no environment name, only ``A3_*``.

    Only :func:`probe.install` is replaced, by the same patches undone after the test.
    """
    monkeypatch.delenv("LOOM_ENV", raising=False)
    monkeypatch.setattr(probe, "_HANDLER", [])
    monkeypatch.setattr(probe, "RECORDER", probe.Recorder())

    def install() -> None:
        for target, name, value in probe.patches():
            monkeypatch.setattr(target, name, value)

    monkeypatch.setattr(probe, "install", install)
    with serving("modern") as server:
        monkeypatch.setenv("A3_MCP_URL", server.url)
        monkeypatch.setenv("A3_JWT_SECRET", _PROBE_SECRET)
        monkeypatch.setenv("A3_CURSOR_KEY", _CURSOR_KEY)
        monkeypatch.setenv("A3_DIRECTORY", str(tmp_path / "a3"))

        handler = probe.lambda_handler
        try:
            record = _ProbeFunction(server, handler, _ThawClock(), capsys).invoke("agent")
        finally:
            handler.close()

    assert record["probe"] == "agent" and record["connects"] == 1
    assert server.runs["echo"] == 1


# ---------------------------------------------------------------------------
# The harness's pure parts
# ---------------------------------------------------------------------------


_SAMPLE: tuple[dict[str, Any], float, float | None] = (
    {
        "probe": "marker",
        "invocation": 7,
        "wall_gap": 1.0,
        "mono_gap": 1.0,
        "connects": 0,
        "mcp_ms": 3.0,
        "init_mcp_ms": None,
    },
    20.0,
    None,
)


class TestTheHarness:
    def test_percentiles_are_nearest_rank(self) -> None:
        samples = [float(n) for n in range(100, 0, -1)]

        assert measure.percentiles(samples) == {"p50": 50.0, "p95": 95.0, "p99": 99.0}

    def test_a_single_sample_is_every_percentile(self) -> None:
        assert measure.percentiles([7.0]) == {"p50": 7.0, "p95": 7.0, "p99": 7.0}

    def test_reads_the_probe_record_from_an_http_response_payload(self) -> None:
        record = {"probe": "marker", "wall_gap": 6.1, "mono_gap": 0.01, "connects": 1}
        payload = json.dumps({"statusCode": 200, "body": json.dumps(record)}).encode()

        assert measure.probe_record(payload) == record

    def test_a_failed_invocation_is_coded_by_its_status_and_error_code(self) -> None:
        body = json.dumps({"code": "internal_error", "message": "An unexpected error occurred"})
        payload = json.dumps({"statusCode": 500, "body": body}).encode()

        with pytest.raises(measure.InvocationFailed) as failed:
            measure.probe_record(payload)

        assert failed.value.code == "500 internal_error"

    def test_a_function_error_is_coded_by_its_error_type(self) -> None:
        payload = json.dumps({"errorType": "Runtime.ImportModuleError"}).encode()

        with pytest.raises(measure.InvocationFailed) as failed:
            measure.probe_record(payload, function_error="Unhandled")

        assert failed.value.code == "Runtime.ImportModuleError"

    def test_failures_are_counted_by_code_and_do_not_end_the_run(self) -> None:
        answers = iter([_SAMPLE, measure.InvocationFailed("500 internal_error"), _SAMPLE])

        def invoke() -> measure.Sample:
            answer = next(answers)
            if isinstance(answer, Exception):
                raise answer
            return answer

        failures: Counter[str] = Counter()
        samples = [measure.attempt(invoke, failures) for _ in range(3)]

        assert samples == [_SAMPLE, None, _SAMPLE]
        report = measure.report("warm", [s for s in samples if s], failures)
        assert report["failures"] == {"500 internal_error": 1}
        assert report["samples"] == 2

    def test_an_expired_sample_that_was_a_cold_start_is_reported_apart(self) -> None:
        """A sample of a new environment measures Init, not an expired session."""
        first_of_its_process = ({**_SAMPLE[0], "invocation": 1}, 9.0, None)
        initialised = (_SAMPLE[0], 9.0, 812.5)

        report = measure.report("expired", [_SAMPLE, first_of_its_process, initialised], Counter())

        assert report["samples"] == 1
        assert report["cold_starts"]["samples"] == 2

    def test_each_cold_start_sets_a_new_value(self) -> None:
        variables = {"A3_MCP_URL": "https://gateway"}

        first = measure.cold_environment(variables)
        second = measure.cold_environment(first)

        assert first["A3_MCP_URL"] == second["A3_MCP_URL"] == "https://gateway"
        assert first["A3_COLD_START"] != second["A3_COLD_START"]
        assert "A3_COLD_START" not in variables

    def test_reads_the_init_duration_from_the_log_tail(self) -> None:
        tail = (
            "START RequestId: 1 Version: $LATEST\n"
            'A3_PROBE {"probe": "marker"}\n'
            "REPORT RequestId: 1\tDuration: 12.50 ms\tBilled Duration: 13 ms\t"
            "Memory Size: 1024 MB\tMax Memory Used: 180 MB\tInit Duration: 1834.27 ms\n"
        )
        encoded = base64.b64encode(tail.encode()).decode()

        assert measure.init_duration_ms(encoded) == 1834.27
        assert measure.init_duration_ms(base64.b64encode(b"REPORT Duration: 1 ms").decode()) is None

    def test_the_era_is_legacy_when_warm_calls_connect(self) -> None:
        assert measure.era([{"connects": 1}, {"connects": 1}]) == "legacy"
        assert measure.era([{"connects": 0}, {"connects": 0}]) == "modern"

    def test_takes_the_function_the_count_and_the_expiry_method(self) -> None:
        argv = ["a3-probe", "--jwt-secret", "s", "--count", "30", "--expiry", "drop"]
        argv += ["--scenario", "expired", "--drop-url", "http://x"]
        args = measure.parse_args(argv)

        assert (args.function, args.count, args.expiry) == ("a3-probe", 30, "drop")
        with pytest.raises(SystemExit):
            measure.parse_args(["a3-probe", "--jwt-secret", "s", "--expiry", "never"])
        with pytest.raises(SystemExit):
            measure.parse_args([*argv[:6], "--scenario", "expired"])  # drop, without its URL


def test_importing_the_probe_patches_nothing() -> None:
    """The probe's patches apply only when installed; production reads no monotonic clock."""
    assert not getattr(AutoBackend.connect_tcp, "a3_probe", False)
    assert not getattr(_mcp._ToolsetSession.list_tools, "a3_probe", False)
    assert "time.monotonic" not in Path(_transport.__file__).read_text(encoding="utf-8")
