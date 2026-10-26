"""``InferenceTarget`` → pydantic-ai model and provider objects (R-002).

One model object is built per plan, at start-up, and reused by every run: the
provider client it carries owns the connection pool, so rebuilding it per
request would pay a new TLS handshake for every prompt.

Each provider is an island under :mod:`loom.ai.engines.pydantic_ai.providers`:
the one module that imports its vendor SDK, at the top, and builds its model.
This module never imports a vendor SDK. It maps ``InferenceTarget.provider``
to the island and the extra that brings it, loads the island once the binding
names it — through :func:`~loom.ai.registry.require_provider_sdk`, the
engine's reading of :func:`loom.core.plugins.optional.import_optional` — and
lets the island do the rest. Adding a vendor is one island and one row here;
an unknown vendor fails at start-up naming the ones this release binds, and a
missing SDK fails naming the extra to install.

Credential conventions, deliberately explicit (FR-018), are each island's to
document: ``bedrock`` reads ``credentials_ref`` as an AWS profile name; the
API-key providers read it as the name of the environment variable holding the
key. ``options`` is handed to the model as pydantic-ai ``ModelSettings`` — the
engine's own vendor-settings vocabulary — so loom introduces no second
settings dialect of its own.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import NamedTuple, Protocol, cast

from pydantic_ai.models import Model

from loom.ai.engines.pydantic_ai.providers._shared import ProviderIsland
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.ai.registry import require_provider_sdk

_ISLANDS = "loom.ai.engines.pydantic_ai.providers"
"""Package every provider island lives in."""


class ModelResolver(Protocol):
    """Builds the engine model that serves one resolved binding.

    The provider takes one as an optional argument so a deployment can supply
    a preconfigured client — and so the shared engine contract suite can run
    this adapter against a scripted model with no network and no credentials
    (FR-048). Production never passes one: the default resolver below is used.
    """

    def __call__(self, target: InferenceTarget) -> Model:
        """Build the model object for ``target``.

        Args:
            target: Resolved model binding carried by the plan.

        Returns:
            The engine model to run with.
        """
        ...


class _Binding(NamedTuple):
    """How one provider identifier resolves to its island.

    Attributes:
        island: Module name of the island under :data:`_ISLANDS`.
        extra: Loom extra whose installation brings the island's SDK.
    """

    island: str
    extra: str


_BINDINGS: Mapping[str, _Binding] = MappingProxyType(
    {
        "bedrock": _Binding("bedrock", "ai-bedrock"),
        "openai": _Binding("openai", "ai-openai"),
        "anthropic": _Binding("anthropic", "ai-anthropic"),
        "gateway": _Binding("gateway", "ai-openai"),
        "typesafe": _Binding("typesafe", "ai-typesafe"),
    }
)

SUPPORTED_PROVIDERS: frozenset[str] = frozenset(_BINDINGS)
"""Provider identifiers this release binds to a pydantic-ai model."""


def _island_for(provider: str) -> ProviderIsland:
    """Load the island of one provider identifier.

    Raises:
        AgentCompilationError: With ``PROVIDER_UNKNOWN`` when this release binds
            no provider of that name, or ``PROVIDER_NOT_INSTALLED`` when its
            SDK is missing, naming the extra.
    """
    binding = _BINDINGS.get(provider)
    if binding is None:
        raise AgentCompilationError(
            [
                AgentErrorCode.PROVIDER_UNKNOWN.issue(
                    component=provider, supported=sorted(SUPPORTED_PROVIDERS)
                )
            ]
        )
    module = require_provider_sdk(provider, f"{_ISLANDS}.{binding.island}", binding.extra)
    # Every island publishes the protocol's two names; the cast records that
    # contract for the type checker, as ``resolve_engine_provider`` does for
    # an engine loaded by entry point.
    return cast(ProviderIsland, module)


def model_class_for(target: InferenceTarget) -> type[Model]:
    """Return the model class a target binds, without building it.

    Answers questions a compiled plan asks about a binding — which
    provider-run tools it admits — with no client, no credential and no request.

    Raises:
        AgentCompilationError: With ``PROVIDER_UNKNOWN`` when no provider of
            that name exists in this release, or ``PROVIDER_NOT_INSTALLED``
            when its SDK is missing.
    """
    return _island_for(target.provider).MODEL_CLASS


def resolve_model(target: InferenceTarget) -> Model:
    """Build the pydantic-ai model bound to one resolved target.

    Args:
        target: Resolved model binding carried by the plan.

    Returns:
        The engine model, with its provider client already configured.

    Raises:
        AgentCompilationError: With ``PROVIDER_UNKNOWN`` when no provider of
            that name exists in this release, with ``PROVIDER_NOT_INSTALLED``
            when the vendor SDK is missing (naming the extra), and with
            ``PROVIDER_SETTING_MISSING`` when a setting the vendor requires is
            absent.
    """
    return _island_for(target.provider).build(target)
