"""OpenAI's own API. ``credentials_ref`` names the variable holding the key."""

from __future__ import annotations

from pydantic_ai.models import Model
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from loom.ai.engines.pydantic_ai.providers._shared import api_key, model_settings, provider_endpoint
from loom.ai.inference import InferenceTarget

MODEL_CLASS: type[Model] = OpenAIChatModel


def build(target: InferenceTarget) -> Model:
    """Build the OpenAI chat model of one binding; ``endpoint`` overrides the base URL."""
    provider = OpenAIProvider(base_url=provider_endpoint(target), api_key=api_key(target))
    return OpenAIChatModel(target.model, provider=provider, settings=model_settings(target))
