"""Spend caps projected onto the engine's own ``UsageLimits``.

``PolicySpec`` carries five caps (FR-040), four of them optional and one,
``max_requests``, always set to a default; :func:`usage_limits` projects all
five one-to-one onto :class:`pydantic_ai.usage.UsageLimits`, built once per
plan at engine construction and passed on every run. An absent optional cap
is ``None``, which the engine treats as "disable that limit".

See "Spend caps" in ``docs/ai/artifacts.md`` for why ``input_tokens_limit``
and ``output_tokens_limit`` are deliberately not projected, for how each
projected limit enforces (preemptive or after the fact), and for why a
model's cost can be permanently unpriceable.
"""

from __future__ import annotations

import logging
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
from pydantic_ai.usage import RequestUsage, RunUsage, UsageLimits

from loom.ai.declarative import PolicySpec
from loom.ai.pricing import ModelPrice

__all__ = ["PricedModel", "is_priceable", "usage_limits", "warn_if_model_not_priceable"]

_logger = logging.getLogger(__name__)


def usage_limits(policies: PolicySpec) -> UsageLimits:
    """Project the artifact's declared spend caps onto the engine's own limits.

    ``count_tokens_before_request`` is deliberately left at its default of
    ``False``, so ``max_input_tokens_per_request`` is enforced against the
    response already received, not against the request it names — see
    ``PolicySpec.max_input_tokens_per_request``.

    Args:
        policies: Validated execution limits carried by the compiled plan.

    Returns:
        ``UsageLimits`` built once per plan; every absent optional cap maps
        to ``None``, which the engine treats as no limit. ``request_limit``
        is never ``None``: ``policies.max_requests`` always carries its
        default.
    """
    return UsageLimits(
        cost_limit=policies.max_usd,
        total_tokens_limit=policies.max_total_tokens,
        per_request_input_tokens_limit=policies.max_input_tokens_per_request,
        tool_calls_limit=policies.max_tool_calls,
        request_limit=policies.max_requests,
    )


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


def warn_if_model_not_priceable(
    model_name: str, provider_name: str | None, policies: PolicySpec, component: str
) -> None:
    """Log a start-up notice when ``policies.max_usd`` may not be enforceable.

    This is a notice, never a boot refusal: ``policies.on_unpriced_spend``
    is what actually governs a run that hits the gap. The probe is fed the
    identifiers the *built* pydantic-ai model reports — its ``model_name``
    and its provider's ``name`` — never loom's own ``InferenceTarget.provider``,
    because billing itself prices on the built model's reported name. Both
    ``LookupError`` and ``ValueError`` from :func:`~genai_prices.calc_price`
    are treated as "not priceable", the same pair pydantic-ai itself treats
    as expected and degrades at run time. See "Spend caps" in
    ``docs/ai/artifacts.md`` for why a model's cost can be permanently
    unpriceable.

    Args:
        model_name: ``model_name`` of the built pydantic-ai model.
        provider_name: ``name`` of the built model's provider, or ``None``
            when the model carries none (a test double, for instance).
        policies: Validated execution limits carried by the compiled plan.
        component: Artifact path or agent name the notice points at.
    """
    if policies.max_usd is None or is_priceable(model_name, provider_name):
        return
    _logger.warning(
        "%s: bound model %r on provider %r cannot be priced by genai-prices "
        "ahead of any run; 'policies.max_usd' may not be enforceable for some "
        "responses, and this deployment's 'policies.on_unpriced_spend' (%r) "
        "governs what a run does when that happens",
        component,
        model_name,
        provider_name or "unknown",
        policies.on_unpriced_spend,
    )
