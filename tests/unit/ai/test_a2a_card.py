"""Unit contract of the A2A card projection and of the A2A event projection.

Both surfaces are pure: :mod:`loom.ai.a2a.card` and :mod:`loom.ai.a2a.events`
derive their output from the compiled :class:`~loom.ai.compiler.AgentPlan`
alone, with no transport library involved.  The suite pins three things the
contract (``specs/001-ai-agent-layer/contracts/a2a.md``) calls out explicitly:

* every projection rule of the card table, field by field;
* the redaction guarantee — the card and the stream say *what* the agent does
  and never *how it is built* (FR-038, FR-030a, SC-009);
* the purity of both modules, so the projection can be unit-tested and reused
  without ``fasta2a``/``fastapi`` on the import path.

The module-level import is deliberate: the red state must come from this
module's own import, never from ``conftest.py``.
"""

from __future__ import annotations

import ast
import base64
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import msgspec
import pytest

from loom.ai.a2a.card import (
    DEFAULT_A2A_PREFIX,
    PROTOCOL_VERSION,
    SKILL_TAGS,
    agent_url,
    build_agent_card,
    card_path,
)
from loom.ai.a2a.events import A2AEventProjector
from loom.ai.abc import (
    AgentEvent,
    AgentUsage,
    ErrorEvent,
    FinalEvent,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.compiler import (
    AgentPlan,
    CompiledA2ACapability,
    CompiledCapability,
    CompiledInstruction,
    CompiledMcpCapability,
    CompiledOutput,
    CompiledPythonCapability,
    CompiledSkillsCapability,
    CompiledSqlCapability,
    CompiledUsecaseCapability,
)
from loom.ai.config import A2AConfig
from loom.ai.declarative import PolicySpec
from loom.ai.errors import AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.model import msgspec_type
from loom.core.model.redaction import Dsn
from loom.core.sql.config import SqlConnectionConfig

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[3]

_BASE_URL: Final[str] = "https://api.example.com"
_AGENT_NAME: Final[str] = "market"
_AGENT_DESCRIPTION: Final[str] = "Analyses the motorcycle market"

_EXPECTED_CARD_KEYS: Final[frozenset[str]] = frozenset(
    {
        "protocolVersion",
        "name",
        "description",
        "url",
        "version",
        "capabilities",
        "defaultInputModes",
        "defaultOutputModes",
        "skills",
        "securitySchemes",
    }
)

_MAX_STEPS: Final[int] = 12
_TASK_ID: Final[str] = "task-1"
_CONTEXT_ID: Final[str] = "ctx-1"

# Every string below is planted in the plan and must never reach the wire.
_CANARY_STRINGS: Final[tuple[str, ...]] = (
    "CANARY-INSTRUCTIONS-do-not-publish",
    "anthropic.claude-canary",
    "eu-canary-1",
    "canary/secret-ref",
    "bedrock",
    "CANARY-OWNER",
    "CANARY-CC",
    "CANARY-123",
    "owner",
    "cost_centre",
    "ticket",
    "canary.usecase.key",
    "canary_server",
    "https://canary-mcp.internal/mcp",
    "canary/mcp-headers",
    "canary_remote",
    "https://canary-remote.internal",
    "canary/a2a-headers",
    "canary-library",
    "/srv/canary-library",
    "canary-skill",
    "myapp.tools:canary_factory",
    "canary_conn",
    "canary-db.internal",
)

# Internal plan sections that must not appear as card keys. ``capabilities`` is
# absent on purpose: the card *does* carry that key, holding the A2A transport
# modes, so its leak is tested by content in ``_CANARY_STRINGS`` instead.
_INTERNAL_KEYS: Final[tuple[str, ...]] = (
    "instructions",
    "inference",
    "policies",
    "metadata",
    "model",
    "provider",
    "region",
    "credentials",
)

_PURE_MODULE_FILES: Final[tuple[str, ...]] = ("card.py", "events.py")
_FORBIDDEN_IMPORT_ROOTS: Final[frozenset[str]] = frozenset(
    {"fasta2a", "a2a", "fastapi", "starlette"}
)


def _canary_factory(container: LoomContainer) -> object:
    """Trivial toolset factory standing in for application-owned code."""
    return container


def _usage() -> AgentUsage:
    """Minimal run accounting for terminal events."""
    return AgentUsage(input_tokens=1, output_tokens=1, requests=1, duration_ms=1)


def _policies() -> PolicySpec:
    """Valid execution limits; every value inside the published ranges."""
    return PolicySpec(
        retries=1,
        tool_timeout_ms=1000,
        max_iterations=_MAX_STEPS,
        run_timeout_ms=30000,
    )


def _output() -> CompiledOutput:
    """Structured-output contract with its boundary type already built."""
    return CompiledOutput(schema={"type": "object"}, loom_type=msgspec_type(dict))


def _plan(**overrides: Any) -> AgentPlan:
    """Build an ``AgentPlan`` with publishable defaults and no internals worth hiding.

    Args:
        **overrides: Plan fields replacing the defaults.

    Returns:
        The compiled plan.
    """
    fields: dict[str, Any] = {
        "name": _AGENT_NAME,
        "description": _AGENT_DESCRIPTION,
        "instructions": (CompiledInstruction(text="Answer using the granted capabilities only."),),
        "spec_version": 1,
        "inference": InferenceTarget(provider="fake", model="fake-model"),
        "output": _output(),
        "capabilities": (),
        "policies": _policies(),
        "metadata": {},
    }
    fields.update(overrides)
    return AgentPlan(**fields)


def _canary_capabilities() -> tuple[CompiledCapability, ...]:
    """One compiled capability of every kind, each carrying a canary internal."""
    return (
        CompiledUsecaseCapability(keys=("canary.usecase.key",), use_cases=()),
        CompiledSqlCapability(
            connection="canary_conn",
            config=SqlConnectionConfig(
                backend="clickhouse",
                url=Dsn("clickhouse://canary-db.internal:8123/reporting"),
            ),
            max_rows=100,
            max_result_bytes=1024,
        ),
        CompiledMcpCapability(
            server="canary_server",
            url="https://canary-mcp.internal/mcp",
            headers_ref="canary/mcp-headers",
        ),
        CompiledSkillsCapability(
            library="canary-library",
            directory="/srv/canary-library",
            names=("canary-skill",),
        ),
        CompiledPythonCapability(
            factory_ref="myapp.tools:canary_factory",
            factory=_canary_factory,
        ),
        CompiledA2ACapability(
            agent="canary_remote",
            url="https://canary-remote.internal",
            headers_ref="canary/a2a-headers",
        ),
    )


def _canary_plan() -> AgentPlan:
    """Plan whose every non-published field carries a recognisable canary."""
    return _plan(
        instructions=(CompiledInstruction(text="CANARY-INSTRUCTIONS-do-not-publish"),),
        inference=InferenceTarget(
            provider="bedrock",
            model="anthropic.claude-canary",
            region="eu-canary-1",
            credentials_ref="canary/secret-ref",
        ),
        capabilities=_canary_capabilities(),
        metadata={
            "owner": "CANARY-OWNER",
            "cost_centre": "CANARY-CC",
            "ticket": "CANARY-123",
        },
    )


def _config(base_url: str = _BASE_URL) -> A2AConfig:
    """A2A exposure config publishing the default agent."""
    return A2AConfig(base_url=base_url, expose=(_AGENT_NAME,))


def _card(
    plan: AgentPlan | None = None,
    *,
    mechanism: str | None = "jwt",
    base_url: str = _BASE_URL,
) -> Mapping[str, object]:
    """Build a card from ``plan`` under the given authentication mechanism."""
    return build_agent_card(plan or _plan(), _config(base_url), mechanism=mechanism)


def _capability_named(card: Mapping[str, object], key: str) -> object:
    """Read one entry of the card's A2A ``capabilities`` mapping."""
    capabilities = card["capabilities"]
    assert isinstance(capabilities, Mapping)
    return capabilities[key]


def _only_skill(card: Mapping[str, object]) -> Mapping[str, object]:
    """Read the single skill the card publishes."""
    skills = card["skills"]
    assert isinstance(skills, Sequence)
    skill = skills[0]
    assert isinstance(skill, Mapping)
    return skill


def _project(events: Sequence[AgentEvent], *, max_steps: int = _MAX_STEPS) -> list[Any]:
    """Project a sequence of agent events through a single projector."""
    projector = A2AEventProjector(task_id=_TASK_ID, context_id=_CONTEXT_ID, max_steps=max_steps)
    projected: list[Any] = []
    for event in events:
        projected.extend(projector.project(event))
    return projected


def _status_of(event: Mapping[str, object]) -> Mapping[str, object]:
    """Read the ``status`` object of a status-update event."""
    status = event["status"]
    assert isinstance(status, Mapping)
    return status


def _texts_in(value: object) -> list[str]:
    """Collect every ``text`` string reachable inside a projected structure."""
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key == "text" and isinstance(item, str):
                found.append(item)
            else:
                found.extend(_texts_in(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.extend(_texts_in(item))
    return found


def _metadata_codes_in(value: object) -> list[object]:
    """Collect every ``metadata.code`` reachable inside a projected structure."""
    found: list[object] = []
    if isinstance(value, Mapping):
        metadata = value.get("metadata")
        if isinstance(metadata, Mapping) and "code" in metadata:
            found.append(metadata["code"])
        for item in value.values():
            found.extend(_metadata_codes_in(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.extend(_metadata_codes_in(item))
    return found


def _import_roots(source_path: Path) -> frozenset[str]:
    """Return the top-level package of every import statement in ``source_path``."""
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".")[0])
    return frozenset(roots)


# ---------------------------------------------------------------------------
# Card projection, rule by rule (contracts/a2a.md, projection table)
# ---------------------------------------------------------------------------


def test_card_declares_the_protocol_version() -> None:
    """The card announces the implemented A2A protocol version."""
    assert _card()["protocolVersion"] == PROTOCOL_VERSION


def test_card_publishes_the_plan_name() -> None:
    """``plan.name`` is published verbatim as the card name."""
    assert _card()["name"] == _AGENT_NAME


def test_card_publishes_the_plan_description() -> None:
    """``plan.description`` is published verbatim as the card description."""
    assert _card()["description"] == _AGENT_DESCRIPTION


def test_card_derives_version_as_a_string_from_spec_version() -> None:
    """``version`` is the string form of ``plan.spec_version``, never an int."""
    assert _card(_plan(spec_version=7))["version"] == "7"


def test_card_derives_url_from_the_configured_base_url() -> None:
    """``url`` is the agent's public endpoint under the A2A prefix."""
    assert _card()["url"] == f"{_BASE_URL}{DEFAULT_A2A_PREFIX}/{_AGENT_NAME}"


def test_card_declares_the_exact_transport_capabilities() -> None:
    """The advertised transport capabilities match what the runtime serves (FR-039b)."""
    assert _card()["capabilities"] == {
        "streaming": True,
        "pushNotifications": False,
        "stateTransitionHistory": False,
    }


@pytest.mark.parametrize("unsupported", ["pushNotifications", "stateTransitionHistory"])
def test_card_does_not_advertise_task_state_capabilities(unsupported: str) -> None:
    """Task-state features are absent, so the card must not advertise them (R-006)."""
    assert _capability_named(_card(), unsupported) is False


def test_card_declares_default_input_modes() -> None:
    """The agent accepts plain text prompts."""
    assert _card()["defaultInputModes"] == ["text/plain"]


def test_card_declares_default_output_modes() -> None:
    """The agent answers with the structured JSON output it was compiled for."""
    assert _card()["defaultOutputModes"] == ["application/json"]


def test_card_publishes_exactly_one_skill() -> None:
    """One plan projects to exactly one skill."""
    skills = _card()["skills"]
    assert isinstance(skills, Sequence)
    assert len(skills) == 1


def test_card_uses_the_plan_name_as_the_skill_id() -> None:
    """``skills[].id`` is the agent name."""
    assert _only_skill(_card())["id"] == _AGENT_NAME


def test_card_uses_the_plan_name_as_the_skill_name() -> None:
    """``skills[].name`` is the agent name."""
    assert _only_skill(_card())["name"] == _AGENT_NAME


def test_card_uses_the_plan_description_as_the_skill_description() -> None:
    """``skills[].description`` is the agent description."""
    assert _only_skill(_card())["description"] == _AGENT_DESCRIPTION


def test_card_uses_fixed_skill_tags_regardless_of_metadata() -> None:
    """``skills[].tags`` is a fixed constant, never derived from ``metadata``."""
    assert _only_skill(_card(_canary_plan()))["tags"] == list(SKILL_TAGS)


@pytest.mark.parametrize(
    ("mechanism", "expected"),
    [
        ("jwt", {"bearer": {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}}),
        ("api-key", {"apiKey": {"type": "apiKey", "in": "header", "name": "X-API-Key"}}),
        ("mtls", {"mutualTLS": {"type": "mutualTLS"}}),
        (None, {}),
        ("something-unknown", {}),
    ],
)
def test_card_derives_security_schemes_from_the_mechanism(
    mechanism: str | None, expected: Mapping[str, object]
) -> None:
    """``securitySchemes`` follows the mechanism in use; an undescribable one is omitted."""
    assert _card(mechanism=mechanism)["securitySchemes"] == expected


def test_card_exposes_exactly_the_contract_key_set() -> None:
    """The card carries the contract's key set and nothing else."""
    assert frozenset(_card().keys()) == _EXPECTED_CARD_KEYS


# ---------------------------------------------------------------------------
# Redaction canary — the card is what a stranger sees (FR-038, SC-009)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("canary", _CANARY_STRINGS)
def test_card_does_not_leak_internal_values_from_the_plan(canary: str) -> None:
    """No instruction, model binding, metadata label or capability wiring reaches the card."""
    assert canary not in json.dumps(_card(_canary_plan(), mechanism="jwt"))


@pytest.mark.parametrize("internal_key", _INTERNAL_KEYS)
def test_card_does_not_expose_internal_plan_sections(internal_key: str) -> None:
    """Internal plan sections are not projected under any key of the card."""
    assert internal_key not in json.dumps(_card(_canary_plan(), mechanism="jwt"))


# ---------------------------------------------------------------------------
# Purity of the projection modules
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("module_file", _PURE_MODULE_FILES)
def test_projection_module_imports_no_transport_dependency(module_file: str) -> None:
    """The projection is pure: no A2A SDK and no web framework on its import path."""
    source = _REPO_ROOT / "src" / "loom" / "ai" / "a2a" / module_file

    assert not (_import_roots(source) & _FORBIDDEN_IMPORT_ROOTS)


# ---------------------------------------------------------------------------
# Event projection (contracts/a2a.md, AgentEvent -> A2A mapping)
# ---------------------------------------------------------------------------


def test_text_delta_projects_a_single_artifact_update() -> None:
    """``text_delta`` maps one-for-one to an artifact update."""
    projected = _project([TextDeltaEvent(text="hello")])

    assert [event["kind"] for event in projected] == ["artifact-update"]


def test_text_delta_publishes_the_text_unmodified() -> None:
    """The model's text is passed through unmodified."""
    assert _texts_in(_project([TextDeltaEvent(text="hello")])) == ["hello"]


def test_text_delta_marks_the_artifact_as_appended_and_open() -> None:
    """Text chunks append to the open artifact and never close it."""
    event = _project([TextDeltaEvent(text="hello")])[0]

    assert (event["append"], event["lastChunk"]) == (True, False)


def test_text_delta_carries_the_task_and_context_ids() -> None:
    """Every projected event carries the task and context ids of the run."""
    event = _project([TextDeltaEvent(text="hello")])[0]

    assert (event["taskId"], event["contextId"]) == (_TASK_ID, _CONTEXT_ID)


def test_tool_call_projects_a_non_final_working_status() -> None:
    """``tool_call`` maps to a non-final ``working`` status update."""
    event = _project([ToolCallEvent(tool="t", call_id="c1", arguments={})])[0]

    assert (event["kind"], _status_of(event)["state"], event["final"]) == (
        "status-update",
        "working",
        False,
    )


def test_tool_call_publishes_only_an_opaque_step_ordinal() -> None:
    """The only text a tool call publishes is its opaque ordinal (FR-030a)."""
    projected = _project([ToolCallEvent(tool="t", call_id="c1", arguments={})])

    assert _texts_in(projected) == [f"step 1/{_MAX_STEPS}"]


@pytest.mark.parametrize("canary", ["canary.usecase.key", "CANARY-ARG", "c1"])
def test_tool_call_does_not_leak_capability_wiring(canary: str) -> None:
    """Neither the capability key, its arguments nor the correlation id are projected."""
    projected = _project(
        [
            ToolCallEvent(
                tool="canary.usecase.key",
                call_id="c1",
                arguments={"secret_arg": "CANARY-ARG"},
            )
        ]
    )

    assert canary not in json.dumps(projected)


def test_step_ordinal_increases_across_consecutive_tool_calls() -> None:
    """Consecutive tool calls are numbered 1-based against the iteration ceiling."""
    calls: list[AgentEvent] = [
        ToolCallEvent(tool="t", call_id=f"c{index}", arguments={}) for index in range(3)
    ]

    assert _texts_in(_project(calls)) == [
        f"step 1/{_MAX_STEPS}",
        f"step 2/{_MAX_STEPS}",
        f"step 3/{_MAX_STEPS}",
    ]


def test_tool_result_projects_a_single_non_final_working_status() -> None:
    """``tool_result`` maps to a single non-final ``working`` status update."""
    projected = _project([ToolResultEvent(call_id="c1", ok=True, summary="done")])

    assert [(event["kind"], _status_of(event)["state"]) for event in projected] == [
        ("status-update", "working")
    ]


def test_tool_result_carries_no_message() -> None:
    """A tool result carries no summary and no payload — only the state change."""
    event = _project([ToolResultEvent(call_id="c1", ok=True, summary="done")])[0]

    assert "message" not in _status_of(event)


@pytest.mark.parametrize("canary", ["CANARY-SUMMARY", "c1"])
def test_tool_result_does_not_leak_summary_or_correlation_id(canary: str) -> None:
    """The tool's outcome summary and correlation id stay inside the process."""
    projected = _project([ToolResultEvent(call_id="c1", ok=True, summary="CANARY-SUMMARY")])

    assert canary not in json.dumps(projected)


def test_final_projects_artifact_then_status_on_success() -> None:
    """``final`` emits the output artifact first, then the terminal status."""
    projected = _project([FinalEvent(output={"answer": 42}, usage=_usage())])

    assert [event["kind"] for event in projected] == ["artifact-update", "status-update"]


def test_final_publishes_the_validated_output() -> None:
    """The validated output travels in the terminal artifact."""
    projected = _project([FinalEvent(output={"answer": 42}, usage=_usage())])

    assert "42" in json.dumps(projected[0])


def test_final_closes_the_stream_as_completed() -> None:
    """The terminal status of a successful run is ``completed`` and final."""
    status_event = _project([FinalEvent(output={"answer": 42}, usage=_usage())])[1]

    assert (_status_of(status_event)["state"], status_event["final"]) == ("completed", True)


def test_final_does_not_publish_the_run_messages() -> None:
    """The run's new messages stay inside the process: no key, no base64 of the bytes."""
    projected = _project([FinalEvent(output={"answer": 42}, usage=_usage(), messages=b"x")])

    encoded = msgspec.json.encode(projected).decode()
    assert "messages" not in encoded
    assert base64.b64encode(b"x").decode() not in encoded


def test_error_projects_a_single_terminal_failed_status() -> None:
    """``error`` emits exactly one terminal ``failed`` status update."""
    projected = _project(
        [ErrorEvent(code=AgentRunErrorCode.TOOL_TIMEOUT, message="tool timed out")]
    )

    assert [(event["kind"], _status_of(event)["state"], event["final"]) for event in projected] == [
        ("status-update", "failed", True)
    ]


def test_error_publishes_the_failure_code_in_metadata() -> None:
    """The stable failure code travels in metadata so a client can branch on it."""
    projected = _project(
        [ErrorEvent(code=AgentRunErrorCode.TOOL_TIMEOUT, message="tool timed out")]
    )

    assert _metadata_codes_in(projected) == [str(AgentRunErrorCode.TOOL_TIMEOUT)]


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------


def test_agent_url_composes_prefix_and_agent_name() -> None:
    """The agent endpoint is the agent name under the A2A prefix."""
    assert agent_url(_BASE_URL, _AGENT_NAME) == f"{_BASE_URL}{DEFAULT_A2A_PREFIX}/{_AGENT_NAME}"


def test_agent_url_ignores_a_trailing_slash_in_the_base_url() -> None:
    """A trailing slash in the configured base URL is a typo, not a different agent."""
    assert agent_url(f"{_BASE_URL}/", _AGENT_NAME) == agent_url(_BASE_URL, _AGENT_NAME)


def test_agent_url_respects_a_custom_prefix() -> None:
    """A deployment may mount the A2A surface under its own prefix."""
    assert agent_url(_BASE_URL, _AGENT_NAME, prefix="/agents") == f"{_BASE_URL}/agents/market"


def test_card_path_is_the_agent_well_known_path() -> None:
    """The card lives at the well-known path under the agent's own prefix."""
    assert card_path(_AGENT_NAME) == (
        f"{DEFAULT_A2A_PREFIX}/{_AGENT_NAME}/.well-known/agent-card.json"
    )


def test_card_path_respects_a_custom_prefix() -> None:
    """The well-known path follows the configured prefix, so the exclusion can match it."""
    assert card_path(_AGENT_NAME, prefix="/agents") == (
        "/agents/market/.well-known/agent-card.json"
    )
