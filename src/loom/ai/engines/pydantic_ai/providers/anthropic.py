"""Anthropic's own API. ``credentials_ref`` names the variable holding the key."""

from __future__ import annotations

from pydantic_ai.models import Model
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.providers.anthropic import AnthropicProvider

from loom.ai.engines.pydantic_ai.providers._shared import api_key, model_settings, provider_endpoint
from loom.ai.inference import InferenceTarget

MODEL_CLASS: type[Model] = AnthropicModel


def build(target: InferenceTarget) -> Model:
    """Build the Anthropic model of one binding; ``endpoint`` overrides the base URL."""
    provider = AnthropicProvider(api_key=api_key(target), base_url=provider_endpoint(target))
    return AnthropicModel(target.model, provider=provider, settings=model_settings(target))
