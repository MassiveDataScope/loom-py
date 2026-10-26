"""Pure projection of a compiled agent into its public A2A agent card.

Implements the projection table of ``specs/001-ai-agent-layer/contracts/a2a.md``
and the purity decision of R-005: the card is derived from the compiled
:class:`~loom.ai.compiler.AgentPlan` alone, with no A2A SDK and no web framework
on the import path, so the redaction guarantee (FR-038, SC-009) is enforced by a
plain unit test in the base wheel instead of an integration test.

The card says *what* the agent does and never *how it is built*: instructions,
model binding, execution policies, free-form metadata and every compiled
capability handle stay inside the process.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from loom.ai.compiler import AgentPlan
from loom.ai.config import A2AConfig

PROTOCOL_VERSION: Final[str] = "1.0.0"
"""A2A protocol version this projection implements."""

DEFAULT_A2A_PREFIX: Final[str] = "/a2a"
"""Path prefix the A2A surface is mounted under unless a deployment overrides it."""

SKILL_TAGS: Final[tuple[str, ...]] = ("agent",)
"""Fixed tags of the published skill.

Constant on purpose: ``plan.metadata`` carries owner, cost centre and ticket
references, so deriving tags from it would leak internals into the card.
"""

_INPUT_MODES: Final[tuple[str, ...]] = ("text/plain",)
_OUTPUT_MODES: Final[tuple[str, ...]] = ("application/json",)

# Advertised transport capabilities must match what the runtime actually serves
# (FR-039b): no persisted task state means no push notifications and no state
# transition history (R-006).
_TRANSPORT_CAPABILITIES: Final[Mapping[str, bool]] = {
    "streaming": True,
    "pushNotifications": False,
    "stateTransitionHistory": False,
}

# One entry per authentication mechanism describable as an A2A security scheme.
# A mechanism absent from this table is published as no scheme at all, never as
# a bearer guess a client would then act on.
_SECURITY_SCHEMES: Final[Mapping[str, Mapping[str, Mapping[str, str]]]] = {
    "jwt": {"bearer": {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}},
    "api-key": {"apiKey": {"type": "apiKey", "in": "header", "name": "X-API-Key"}},
    "mtls": {"mutualTLS": {"type": "mutualTLS"}},
}


def _security_schemes(mechanism: str | None) -> Mapping[str, Mapping[str, str]]:
    if mechanism is None:
        return {}
    return dict(_SECURITY_SCHEMES.get(mechanism, {}))


def agent_url(base_url: str, name: str, *, prefix: str = DEFAULT_A2A_PREFIX) -> str:
    """Build the public endpoint of one agent.

    Args:
        base_url: Public base URL of the deployment; a trailing slash is a typo
            and is ignored rather than producing a different endpoint.
        name: Agent name.
        prefix: Path prefix the A2A surface is mounted under.

    Returns:
        The absolute URL clients send A2A requests to.

    Example::

        agent_url("https://api.example.com", "market")  # .../a2a/market
    """
    return f"{base_url.rstrip('/')}{prefix}/{name}"


def card_path(name: str, *, prefix: str = DEFAULT_A2A_PREFIX) -> str:
    """Build the well-known path serving one agent's card.

    The path is per agent so the authentication exclusion registered for it
    matches the card alone and never the invocation surface (FR-041b).

    Args:
        name: Agent name.
        prefix: Path prefix the A2A surface is mounted under.

    Returns:
        The path of the agent card, relative to the deployment root.
    """
    return f"{prefix}/{name}/.well-known/agent-card.json"


def _skill(plan: AgentPlan) -> Mapping[str, object]:
    return {
        "id": plan.name,
        "name": plan.name,
        "description": plan.description,
        "tags": list(SKILL_TAGS),
    }


def build_agent_card(
    plan: AgentPlan,
    config: A2AConfig,
    *,
    mechanism: str | None,
    prefix: str = DEFAULT_A2A_PREFIX,
) -> Mapping[str, object]:
    """Project a compiled agent into its public A2A agent card.

    Only the agent's identity, its endpoint, the modes actually served and the
    security scheme in use are published. Instructions, model binding, region,
    credentials, policies, metadata and capability wiring are never projected
    (FR-038, SC-009).

    Args:
        plan: Compiled agent to describe.
        config: A2A exposure settings supplying the deployment's base URL.
        mechanism: Authentication mechanism the endpoint enforces; ``None``, or
            one with no A2A representation, publishes no security scheme.
        prefix: Path prefix the A2A surface is mounted under.

    Returns:
        The agent card, ready to be serialised as JSON.

    Example::

        card = build_agent_card(plan, config, mechanism="jwt")
    """
    return {
        "protocolVersion": PROTOCOL_VERSION,
        "name": plan.name,
        "description": plan.description,
        "url": agent_url(config.base_url.reveal(), plan.name, prefix=prefix),
        "version": str(plan.spec_version),
        "capabilities": dict(_TRANSPORT_CAPABILITIES),
        "defaultInputModes": list(_INPUT_MODES),
        "defaultOutputModes": list(_OUTPUT_MODES),
        "skills": [_skill(plan)],
        "securitySchemes": _security_schemes(mechanism),
    }
