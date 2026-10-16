"""Configured rates applied to every response, and whether genai-prices prices a model.

:class:`PricedModel` sets the cost of each response from ``ai.prices``;
:func:`is_priceable` probes genai-prices for the built model ahead of any run.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from typing import Any

from genai_prices import calc_price
from pydantic_ai import RunContext
from pydantic_ai.messages import ModelMessage, ModelResponse
from pydantic_ai.models import Model, ModelRequestParameters, StreamedResponse
from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.settings import ModelSettings
from pydantic_ai.usage import RequestUsage, RunUsage

from loom.ai.pricing import ModelPrice

__all__ = ["PricedModel", "is_priceable"]


class PricedModel(WrapperModel):
    """Model whose every response is priced with the deployment's own rates.

    The cost is set on the response the wrapped model returns, before the
    engine records it in the run's usage, so the engine's ``cost_limit`` and
    loom's ``on_unpriced_spend`` both read it, whatever the engine version
    does after the response leaves the model. It replaces any cost the model
    reported and whatever genai-prices would have computed.

    Args:
        wrapped: Model built for the plan's binding.
        price: Rates of that model.
    """

    def __init__(self, wrapped: Model, price: ModelPrice) -> None:
        super().__init__(wrapped)
        self.price = price

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        """Return the wrapped model's response with its usage priced at :attr:`price`."""
        response = await super().request(messages, model_settings, model_request_parameters)
        return replace(response, usage=replace(response.usage, cost=self._cost(response.usage)))

    @asynccontextmanager
    async def request_stream(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
        run_context: RunContext[Any] | None = None,
    ) -> AsyncIterator[StreamedResponse]:
        """Yield the wrapped model's stream, its usage priced at :attr:`price` once it closes."""
        async with super().request_stream(
            messages, model_settings, model_request_parameters, run_context
        ) as stream:
            try:
                yield stream
            finally:
                stream.usage.cost = self._cost(stream.usage)

    def _cost(self, usage: RequestUsage) -> Decimal:
        return self.price.cost(
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            cache_read_tokens=usage.cache_read_tokens,
            cache_write_tokens=usage.cache_write_tokens,
        )


def is_priceable(model_name: str, provider_name: str | None) -> bool:
    """Report whether genai-prices can price the built model ahead of any run.

    Args:
        model_name: ``model_name`` of the built pydantic-ai model.
        provider_name: ``name`` of the built model's provider, or ``None``.

    Returns:
        ``False`` when genai-prices raises ``LookupError`` or ``ValueError``.
    """
    try:
        calc_price(RunUsage(), model_name, provider_id=provider_name)
    except (LookupError, ValueError):
        return False
    return True
