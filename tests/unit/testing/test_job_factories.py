"""``make`` builds every job value object from its own type rules and registered factories."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.job.delivery import JobDelivery
from loom.core.job.envelope import JobEnvelope, wire_job_name
from loom.core.job.outcome import Abandoned, Failed, Postponed, Succeeded
from loom.core.job.policy import Backoff, Countdown, RetryPolicy
from loom.testing import NoopJob, SampleFactory, make

BUILDS = 200


class TestAnnotatedRules:
    @pytest.mark.parametrize("model", [Countdown, Backoff, RetryPolicy])
    def test_every_generated_value_passes_the_type_checks(self, model: type[Any]) -> None:
        built = [
            SampleFactory.create_factory(model, __random_seed__=seed).build()
            for seed in range(BUILDS)
        ]

        assert all(isinstance(value, model) for value in built)


class TestRegisteredFactories:
    def test_a_delivery_is_the_first_one_of_the_noop_job(self) -> None:
        delivery = make(JobDelivery)

        assert (delivery.job_type, delivery.attempt, delivery.postponements) == (NoopJob, 1, 0)

    def test_a_delivery_nests_an_envelope_naming_its_job(self) -> None:
        delivery = make(JobDelivery)

        assert delivery.envelope.job == wire_job_name(NoopJob.__qualname__)

    def test_a_given_field_wins_over_the_factory(self) -> None:
        assert make(JobEnvelope, task_id="given").task_id == "given"

    @pytest.mark.parametrize("outcome", [Succeeded, Failed, Abandoned, Postponed])
    def test_every_outcome_builds(self, outcome: type[Any]) -> None:
        assert isinstance(make(outcome), outcome)
