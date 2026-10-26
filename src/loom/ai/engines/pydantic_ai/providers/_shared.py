"""What every provider island shares: its contract, and the two readers of a binding."""

from __future__ import annotations

from typing import Protocol, cast, runtime_checkable

from pydantic_ai.models import Model
from pydantic_ai.settings import ModelSettings

from loom.ai.inference import InferenceTarget
from loom.ai.registry import require_provider_setting
from loom.core.environment import configured_variable


@runtime_checkable
class ProviderIsland(Protocol):
    """The two names a provider island publishes.

    Attributes:
        MODEL_CLASS: The pydantic-ai model class the island builds, for the
            questions a compiled plan asks about a binding with no client
            built — which provider-run tools it admits.
    """

    MODEL_CLASS: type[Model]

    def build(self, target: InferenceTarget) -> Model:
        """Build the configured model of one resolved binding."""
        ...


def model_settings(target: InferenceTarget) -> ModelSettings | None:
    """Vendor settings as the engine's own ``ModelSettings`` mapping."""
    if not target.options:
        return None
    # ``ModelSettings`` is a total=False TypedDict; the values come from
    # deployment configuration and cannot be named statically.
    return cast(ModelSettings, dict(target.options))


def provider_endpoint(target: InferenceTarget) -> str | None:
    """Return the endpoint string the provider SDK connects to.

    ``None`` leaves the SDK on its own default. The stored address stays a
    :class:`~loom.core.model.redaction.Dsn`, whose text form hides a password.
    """
    if target.endpoint is None:
        return None
    return target.endpoint.reveal()


def api_key(target: InferenceTarget) -> str | None:
    """Read the API key of a target whose ``credentials_ref`` names a variable.

    ``credentials_ref`` is the name of an environment variable, never the key
    itself, so a deployment declares where the secret lives instead of putting
    it in configuration. Without it the provider SDK reads its own default
    variable.

    Raises:
        AgentCompilationError: With ``PROVIDER_SETTING_MISSING`` when the named
            variable is unset or empty.
        ConfigError: When ``credentials_ref`` names a ``LOOM_*`` variable.
    """
    reference = target.credentials_ref
    if reference is None:
        return None
    value = configured_variable(reference)
    require_provider_setting(target.provider, f"credentials_ref ({reference})", value)
    return value
