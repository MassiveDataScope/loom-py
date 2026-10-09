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
from dataclasses import dataclass, replace
from typing import Any

from genai_prices import calc_price
from pydantic_ai import RunContext
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.messages import ModelResponse
from pydantic_ai.models import ModelRequestContext
from pydantic_ai.usage import RunUsage, UsageLimits

from loom.ai.declarative import PolicySpec
from loom.ai.pricing import ModelPrice

__all__ = ["ConfiguredPrice", "is_priceable", "usage_limits", "warn_if_model_not_priceable"]

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


@dataclass
class ConfiguredPrice(AbstractCapability[Any]):
    """Price every model response with the deployment's own rates.

    The cost is set before the response joins the run's usage, so the
    engine's ``cost_limit`` and loom's ``on_unpriced_spend`` both read it,
    and it replaces whatever genai-prices would have computed.

    Args:
        price: Rates of the model the plan is bound to.
    """

    price: ModelPrice

    async def after_model_request(
        self,
        ctx: RunContext[Any],
        *,
        request_context: ModelRequestContext,
        response: ModelResponse,
    ) -> ModelResponse:
        """Return *response* with its usage priced at :attr:`price`."""
        usage = response.usage
        cost = self.price.cost(
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            cache_read_tokens=usage.cache_read_tokens,
            cache_write_tokens=usage.cache_write_tokens,
        )
        return replace(response, usage=replace(usage, cost=cost))


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
