"""Spend caps projected onto the engine (spec 016, T501/T503).

``usage_limits`` is a pure function over ``PolicySpec``;
``warn_if_model_not_priceable`` is a pure function over the identifiers a
*built* pydantic-ai model reports — never over ``InferenceTarget`` (see its
own docstring for why). These tests inspect what reaches the engine's own
``UsageLimits`` directly, rather than inferring it from a run's outcome
(plan.md's verification strategy).

The pricing probe no longer refuses to boot (see the elastic-policy design
note in ``engines/pydantic_ai/_limits.py``): what a run does about an
unpriced response is ``policies.on_unpriced_spend``'s decision, made and
enforced at run time (``tests/unit/ai/engines/test_pydantic_ai_cost_enforcement.py``).
This probe only leaves a start-up trail an operator can find before a run
ever happens.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest
from pydantic_ai.models import Model
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.models.test import TestModel
from pydantic_ai.providers.openai import OpenAIProvider

from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai import _limits
from loom.ai.engines.pydantic_ai._limits import usage_limits, warn_if_model_not_priceable
from loom.ai.engines.pydantic_ai._models import ModelResolver
from loom.ai.engines.pydantic_ai.provider import PydanticAIEngineProvider
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.model.redaction import Dsn
from tests.helpers.pydantic_ai_engine import NullDeps, make_plan

_COMPONENT = "agent 'appraiser'"
_LIMITS_LOGGER = "loom.ai.engines.pydantic_ai._limits"


def _gateway_reported_as_openai(target: InferenceTarget) -> Model:
    """Stand in for ``_gateway_model``: same provider class, no network at init.

    A ``gateway`` binding builds an ``OpenAIProvider`` and reaches it through
    ``OpenAIChatModel``, exactly as the ``gateway`` provider island does; the
    real model name travels through, only the credentials are stubbed.
    """
    return OpenAIChatModel(target.model, provider=OpenAIProvider(api_key="test-key"))


class TestUsageLimitsProjection:
    """Each of the five caps projects onto its own engine field, and no other."""

    def test_absent_policies_produce_the_engines_own_fifty_and_no_other_cap(self) -> None:
        """AC-010: a plan with no declared caps carries today's behaviour, written down."""
        limits = usage_limits(PolicySpec())

        assert limits.request_limit == 50
        assert limits.cost_limit is None
        assert limits.total_tokens_limit is None
        assert limits.per_request_input_tokens_limit is None
        assert limits.tool_calls_limit is None

    def test_every_declared_cap_reaches_its_own_engine_field(self) -> None:
        """Every declared cap reaches the exact engine field FR-040 names."""
        policies = PolicySpec(
            max_usd=Decimal("2.00"),
            max_total_tokens=50000,
            max_input_tokens_per_request=12000,
            max_tool_calls=30,
            max_requests=25,
        )

        limits = usage_limits(policies)

        assert limits.cost_limit == Decimal("2.00")
        assert limits.total_tokens_limit == 50000
        assert limits.per_request_input_tokens_limit == 12000
        assert limits.tool_calls_limit == 30
        assert limits.request_limit == 25

    def test_max_usd_reaches_cost_limit_as_a_decimal(self) -> None:
        """``max_usd`` never passes through binary float on its way to ``cost_limit``.

        The projection is a direct assignment, so identity — not equality —
        is the assertion that catches a regression: ``Decimal(float(...))``
        still compares equal to the original ``Decimal`` (``==`` on
        ``Decimal`` is numeric and ignores the exponent) and is still a
        ``Decimal``, so only ``is`` proves no conversion happened.
        """
        policies = PolicySpec(max_usd=Decimal("2.00"))

        limits = usage_limits(policies)

        assert limits.cost_limit is policies.max_usd

    def test_max_input_tokens_per_request_never_flips_count_tokens_before_request(self) -> None:
        """``OpenAIChatModel`` (``openai``/``gateway``) has no ``count_tokens``.

        Flipping this flag whenever the cap is declared would raise
        ``NotImplementedError`` on the very first request for every model
        class that does not implement token counting ahead of a request —
        most of this release's providers. The gap this cap leaves is
        documented (``PolicySpec.max_input_tokens_per_request``), not closed
        by a flag this module cannot safely set unconditionally.
        """
        limits = usage_limits(PolicySpec(max_input_tokens_per_request=12000))

        assert limits.count_tokens_before_request is False


class TestPricingProbe:
    """The pricing probe logs a start-up notice; it never refuses to boot.

    The probe is fed the identifiers the *built* pydantic-ai model reports —
    never ``InferenceTarget.provider`` — so every case here passes a model
    name and a provider name directly, the same two strings
    :meth:`~loom.ai.engines.pydantic_ai.provider.PydanticAIEngineProvider.create_engine`
    reads off the resolved model.
    """

    def test_no_warning_when_max_usd_is_absent(self, caplog: pytest.LogCaptureFixture) -> None:
        """No probe runs at all when the artifact declares no cost cap."""
        with caplog.at_level(logging.WARNING, logger=_LIMITS_LOGGER):
            warn_if_model_not_priceable(
                "anything-goes-unchecked", "openai", PolicySpec(), _COMPONENT
            )

        assert caplog.records == []

    def test_no_warning_when_the_bound_model_can_be_priced(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A model ``genai-prices`` knows about raises no notice with ``max_usd`` declared."""
        with caplog.at_level(logging.WARNING, logger=_LIMITS_LOGGER):
            warn_if_model_not_priceable(
                "gpt-4o", "openai", PolicySpec(max_usd=Decimal("2.00")), _COMPONENT
            )

        assert caplog.records == []

    def test_warns_when_the_model_cannot_be_priced(self, caplog: pytest.LogCaptureFixture) -> None:
        """An unknown model logs a notice naming the artifact, the model and the provider."""
        with caplog.at_level(logging.WARNING, logger=_LIMITS_LOGGER):
            warn_if_model_not_priceable(
                "totally-unknown-model-xyz",
                "openai",
                PolicySpec(max_usd=Decimal("2.00")),
                _COMPONENT,
            )

        assert len(caplog.records) == 1
        message = caplog.records[0].getMessage()
        assert _COMPONENT in message
        assert "totally-unknown-model-xyz" in message
        assert "openai" in message

    def test_warns_when_the_reported_provider_is_unknown_to_genai_prices(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A provider name genai-prices has never heard of also triggers a notice."""
        with caplog.at_level(logging.WARNING, logger=_LIMITS_LOGGER):
            warn_if_model_not_priceable(
                "gpt-4o",
                "a-provider-genai-prices-has-never-heard-of",
                PolicySpec(max_usd=Decimal("2.00")),
                _COMPONENT,
            )

        assert len(caplog.records) == 1

    def test_warns_when_the_model_carries_no_provider(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A model with no provider (a test double) probes with ``provider_id=None``."""
        with caplog.at_level(logging.WARNING, logger=_LIMITS_LOGGER):
            warn_if_model_not_priceable(
                "totally-unknown-model-xyz", None, PolicySpec(max_usd=Decimal("2.00")), _COMPONENT
            )

        assert "unknown" in caplog.records[0].getMessage()

    def test_warns_when_the_probe_raises_value_error(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """``ValueError`` is caught too: ``best_effort_price`` degrades on both."""

        def _raise_value_error(*args: object, **kwargs: object) -> object:
            raise ValueError("usage data genai-prices cannot price")

        monkeypatch.setattr(_limits, "calc_price", _raise_value_error)

        with caplog.at_level(logging.WARNING, logger=_LIMITS_LOGGER):
            warn_if_model_not_priceable(
                "gpt-4o", "openai", PolicySpec(max_usd=Decimal("2.00")), _COMPONENT
            )

        assert len(caplog.records) == 1


def _create_engine(
    policies: PolicySpec,
    *,
    inference: InferenceTarget | None = None,
    model_resolver: ModelResolver | None = None,
) -> None:
    resolve = model_resolver or (lambda target: TestModel(model_name=target.model))
    provider = PydanticAIEngineProvider(model_resolver=resolve)
    plan = make_plan(policies=policies, inference=inference)
    provider.create_engine(plan, deps=NullDeps(), container=LoomContainer())


class TestCreateEngineWiring:
    """``create_engine`` (T503) resolves the model before running the pricing probe.

    The probe must see what the *resolved* model reports, so these wire the
    whole provider rather than calling ``warn_if_model_not_priceable`` in
    isolation.
    """

    def test_builds_when_the_resolved_model_cannot_be_priced(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """An unpriceable model no longer blocks the build — it only leaves a notice."""
        unpriceable = InferenceTarget(provider="openai", model="totally-unknown-model-xyz")

        with caplog.at_level(logging.WARNING, logger=_LIMITS_LOGGER):
            _create_engine(PolicySpec(max_usd=Decimal("2.00")), inference=unpriceable)

        assert len(caplog.records) == 1
        assert "totally-unknown-model-xyz" in caplog.records[0].getMessage()

    def test_builds_clean_when_max_usd_is_absent(self) -> None:
        _create_engine(PolicySpec())

    def test_builds_clean_for_a_gateway_binding_priced_as_the_provider_it_resolves_to(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A ``gateway`` binding is priced as ``openai``, the name pydantic-ai reports.

        Probing with loom's own ``InferenceTarget.provider`` ("gateway") would
        warn here, even though every run bills correctly through the
        ``OpenAIProvider`` the binding actually builds.
        """
        gateway = InferenceTarget(
            provider="gateway", model="gpt-4o", endpoint=Dsn("https://gw.test")
        )

        with caplog.at_level(logging.WARNING, logger=_LIMITS_LOGGER):
            _create_engine(
                PolicySpec(max_usd=Decimal("2.00")),
                inference=gateway,
                model_resolver=_gateway_reported_as_openai,
            )

        assert caplog.records == []
