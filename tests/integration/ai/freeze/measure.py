"""The A3 harness (plan 6.8): invoke a deployed probe function and report p50, p95, p99.

The function runs :mod:`tests.integration.ai.freeze.probe` unchanged. Each
invocation is a direct ``Invoke`` of an API Gateway (v2) request to the probe;
the probe's record comes back as the response body. Three scenarios:

* ``warm``: *count* invocations back to back (A3 uses 500), each a warm
  invocation; reports the probe's ``mcp_ms`` and the round trip, the gaps, the
  connects per call, and the protocol era they imply (connects on a warm call
  mean the legacy era, which opens one connection per ``POST``);
* ``expired``: *count* samples (A3 uses 30) of the first call after an expired
  session; the session is expired by the gateway's TTL (``--expiry ttl``: wait
  ``--idle`` seconds) or by a server-side drop (``--expiry drop``: ``POST``
  ``--drop-url``, which the test server answers by forgetting its sessions);
* ``cold``: *count* cold starts (A3 uses 30): each sets an environment
  variable of the function to a value never used before, which makes the next
  invocation cold, and reports the probe's ``init_mcp_ms`` and Lambda's
  ``Init Duration``.

An ``expired`` sample that turns out to be a cold start (the probe's first
invocation, or an ``Init Duration`` in the log tail) is reported apart, under
``cold_starts``. A failed invocation does not end the run: failures are
counted per scenario by their code (HTTP status and error ``code``, or
Lambda's ``errorType``) and reported under ``failures``.

Usage::

    uv run python -m tests.integration.ai.freeze.measure a3-probe --jwt-secret "$S" \\
        --scenario warm --count 500
    uv run python -m tests.integration.ai.freeze.measure a3-probe --jwt-secret "$S" \\
        --scenario expired --count 30 --expiry ttl --idle 330

boto3 is imported only by :func:`main`; nothing here runs on import.
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import re
import time
import urllib.request
import uuid
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from typing import Any

_INIT_DURATION = re.compile(r"Init Duration: ([0-9.]+) ms")
_COLD_VARIABLE = "A3_COLD_START"
_AUDIENCE = "a3-probe"
_ISSUER = "a3-harness"


Sample = tuple[dict[str, Any], float, float | None]
"""One invocation: the probe's record, the round trip in ms, and any ``Init Duration``."""


class InvocationFailed(RuntimeError):
    """The function answered with something other than a probe record.

    Attributes:
        code: What failed, counted per scenario: the HTTP status and the error
            envelope's ``code``, or Lambda's ``errorType``.
    """

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code


def percentiles(samples: Sequence[float]) -> dict[str, float]:
    """p50, p95 and p99 of *samples*, by the nearest-rank method."""
    ordered = sorted(samples)

    def rank(p: int) -> float:
        return ordered[max(math.ceil(p / 100 * len(ordered)), 1) - 1]

    return {"p50": rank(50), "p95": rank(95), "p99": rank(99)}


def probe_record(payload: bytes, *, function_error: str | None = None) -> dict[str, Any]:
    """The probe's record, from an ``Invoke`` response payload of an HTTP request.

    Raises:
        InvocationFailed: When the function failed (*function_error*, Lambda's
            ``FunctionError``) or answered with a status other than 200.
    """
    response = json.loads(payload)
    if function_error is not None:
        raise InvocationFailed(response.get("errorType", function_error), str(response))
    status, body = response.get("statusCode"), response.get("body")
    if status != 200:
        envelope = json.loads(body) if body else {}
        code = envelope.get("code") if isinstance(envelope, dict) else None
        raise InvocationFailed(f"{status} {code}" if code else str(status), str(body))
    return json.loads(body)


def init_duration_ms(log_result: str) -> float | None:
    """Lambda's ``Init Duration`` from a base64 log tail; ``None`` on a warm invocation."""
    found = _INIT_DURATION.search(base64.b64decode(log_result).decode(errors="replace"))
    return float(found.group(1)) if found else None


def attempt(invoke: Callable[[], Sample], failures: Counter[str]) -> Sample | None:
    """*invoke* once; a failure is counted by its code instead of ending the run."""
    try:
        return invoke()
    except InvocationFailed as failed:
        failures[failed.code] += 1
        return None


def cold_environment(variables: Mapping[str, str]) -> dict[str, str]:
    """*variables* with a value never set before, so the update starts new environments."""
    return {**variables, _COLD_VARIABLE: uuid.uuid4().hex}


def _cold(sample: Sample) -> bool:
    """Whether *sample* is its process's first invocation, which ran after Init."""
    record, _, init_duration = sample
    return record["invocation"] == 1 or init_duration is not None


def report(scenario: str, samples: Sequence[Sample], failures: Counter[str]) -> dict[str, Any]:
    """The scenario's summary.

    An ``expired`` sample that was a cold start measures Init rather than an
    expired session (the environment was recycled while idle), so it is
    reported apart, under ``cold_starts``.
    """
    measured = list(samples)
    summary: dict[str, Any] = {"scenario": scenario}
    if scenario == "expired":
        cold = [sample for sample in measured if _cold(sample)]
        measured = [sample for sample in measured if not _cold(sample)]
        if cold:
            summary["cold_starts"] = _summary(cold)
    summary |= _summary(measured)
    summary["failures"] = dict(failures)
    if scenario == "warm" and measured:
        summary["era"] = era([record for record, _, _ in measured])
    return summary


def era(records: Sequence[dict[str, Any]]) -> str:
    """The protocol era warm calls imply: legacy when they open connections."""
    return "legacy" if any(record["connects"] for record in records) else "modern"


def bearer(secret: str, *, lifetime: float = 24 * 3600) -> dict[str, str]:
    """The ``Authorization`` header of a caller the probe accepts."""
    import jwt

    claims = {
        "sub": "a3-harness",
        "aud": _AUDIENCE,
        "iss": _ISSUER,
        "exp": int(time.time() + lifetime),
    }
    return {"authorization": "Bearer " + jwt.encode(claims, secret, algorithm="HS256")}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure A3 on a deployed probe function.")
    parser.add_argument("function", help="the deployed probe function's name or ARN")
    parser.add_argument("--count", type=int, default=500, help="invocations or samples")
    parser.add_argument("--expiry", choices=("ttl", "drop"), default="ttl")
    parser.add_argument("--scenario", choices=("warm", "expired", "cold"), default="warm")
    parser.add_argument("--path", choices=("marker", "agent"), default="marker")
    parser.add_argument("--idle", type=float, default=330.0, help="seconds, --expiry ttl")
    parser.add_argument("--drop-url", help="URL a POST to drops the server's sessions")
    parser.add_argument("--arguments", default='{"text": "a3"}', help="the tool's JSON args")
    parser.add_argument("--jwt-secret", required=True, help="the probe's A3_JWT_SECRET")
    parser.add_argument("--region")
    args = parser.parse_args(argv)
    if args.scenario == "expired" and args.expiry == "drop" and not args.drop_url:
        parser.error("--expiry drop needs --drop-url")
    return args


class _Function:
    def __init__(self, client: Any, args: argparse.Namespace) -> None:
        from loom.testing import LambdaEvent

        self._client, self._name = client, args.function
        event = LambdaEvent.http(
            "POST",
            f"/a3/{args.path}",
            headers=bearer(args.jwt_secret),
            json_body={"arguments": json.loads(args.arguments)},
        )
        self._event = json.dumps(event).encode()

    def invoke(self) -> Sample:
        """One invocation; raises :class:`InvocationFailed` when it did not answer a record."""
        started = time.perf_counter()
        answer = self._client.invoke(FunctionName=self._name, Payload=self._event, LogType="Tail")
        elapsed = (time.perf_counter() - started) * 1000
        payload = answer["Payload"].read()
        record = probe_record(payload, function_error=answer.get("FunctionError"))
        return record, elapsed, init_duration_ms(answer.get("LogResult", ""))

    def make_cold(self) -> None:
        config = self._client.get_function_configuration(FunctionName=self._name)
        variables = config.get("Environment", {}).get("Variables", {})
        self._client.update_function_configuration(
            FunctionName=self._name, Environment={"Variables": cold_environment(variables)}
        )
        self._client.get_waiter("function_updated_v2").wait(FunctionName=self._name)


def _expire(args: argparse.Namespace) -> None:
    if args.expiry == "ttl":
        time.sleep(args.idle)
        return
    request = urllib.request.Request(args.drop_url, method="POST")  # noqa: S310
    urllib.request.urlopen(request, timeout=10).close()  # noqa: S310


def _summary(samples: Sequence[Sample]) -> dict[str, Any]:
    records = [record for record, _, _ in samples]
    if not records:
        return {"samples": 0}
    gaps = [(record["wall_gap"], record["mono_gap"]) for record in records]
    summary: dict[str, Any] = {
        "samples": len(samples),
        "mcp_ms": percentiles([record["mcp_ms"] for record in records]),
        "round_trip_ms": percentiles([elapsed for _, elapsed, _ in samples]),
        "connects_per_call": dict(Counter(record["connects"] for record in records)),
        "gaps": [gap for gap in gaps if gap[0] is not None],
    }
    inits = [record["init_mcp_ms"] for record in records if record["init_mcp_ms"] is not None]
    durations = [duration for _, _, duration in samples if duration is not None]
    if inits:
        summary["init_mcp_ms"] = percentiles(inits)
    if durations:
        summary["init_duration_ms"] = percentiles(durations)
    return summary


def main(argv: Sequence[str] | None = None) -> None:
    import boto3

    args = parse_args(argv)
    client = boto3.client("lambda", region_name=args.region)
    function = _Function(client, args)
    failures: Counter[str] = Counter()
    if args.scenario == "warm":
        attempt(function.invoke, failures)  # Init's connects and the first call are not warm
    samples: list[Sample] = []
    for _ in range(args.count):
        if args.scenario == "cold":
            function.make_cold()
        elif args.scenario == "expired":
            attempt(function.invoke, failures)  # a live session to expire
            _expire(args)
        if (sample := attempt(function.invoke, failures)) is not None:
            samples.append(sample)
    print(json.dumps({"path": args.path, **report(args.scenario, samples, failures)}, indent=2))


if __name__ == "__main__":
    main()
