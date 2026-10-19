"""TypeSafe's Jev, a decision model: it fills a typed output from a state and never writes text.

pydantic-ai fills that output through tool mode only, so a binding pinning
``output_mode: native`` is refused here, at start-up, instead of by the model
on every request. ``credentials_ref`` names the variable holding the key.
"""

from __future__ import annotations

from pydantic_ai.models import Model
from pydantic_ai.models.typesafe import TypeSafeModel
from pydantic_ai.providers.typesafe import TypeSafeProvider

from loom.ai.engines.pydantic_ai.providers._shared import api_key, model_settings
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget

MODEL_CLASS: type[Model] = TypeSafeModel

_OUTPUT_MODES: tuple[str, ...] = ("tool",)
"""The output modes pydantic-ai serves on a TypeSafe model: it fills an output by tool only."""


def build(target: InferenceTarget) -> Model:
    """Build the TypeSafe model of one binding; ``endpoint`` overrides the base URL."""
    if target.output_mode is not None and target.output_mode not in _OUTPUT_MODES:
        raise AgentCompilationError(
            [
                AgentErrorCode.OUTPUT_MODE_UNSUPPORTED.issue(
                    component="typesafe", mode=target.output_mode, supported=_OUTPUT_MODES
                )
            ]
        )
    provider = TypeSafeProvider(api_key=api_key(target), base_url=target.endpoint)
    return TypeSafeModel(target.model, provider=provider, settings=model_settings(target))
