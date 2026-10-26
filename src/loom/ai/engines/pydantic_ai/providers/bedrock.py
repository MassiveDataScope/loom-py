"""AWS Bedrock. ``credentials_ref`` is an AWS profile name; absent, the boto3 chain applies."""

from __future__ import annotations

from pydantic_ai.models import Model
from pydantic_ai.models.bedrock import BedrockConverseModel
from pydantic_ai.providers.bedrock import BedrockProvider

from loom.ai.engines.pydantic_ai.providers._shared import model_settings, provider_endpoint
from loom.ai.inference import InferenceTarget
from loom.ai.registry import require_provider_setting

MODEL_CLASS: type[Model] = BedrockConverseModel


def build(target: InferenceTarget) -> Model:
    """Build the Bedrock model of one binding; ``region`` is required."""
    require_provider_setting("bedrock", "region", target.region)
    provider = BedrockProvider(
        region_name=target.region,
        profile_name=target.credentials_ref,
        base_url=provider_endpoint(target),
    )
    return BedrockConverseModel(target.model, provider=provider, settings=model_settings(target))
