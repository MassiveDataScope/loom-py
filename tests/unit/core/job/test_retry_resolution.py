from __future__ import annotations

from typing import Any

import pytest
from omegaconf import OmegaConf

from loom.core.config import ConfigContext, ConfigError
from loom.core.job.config import JobSettings, RetrySource
from loom.core.job.job import Job
from loom.core.job.policy import DEFAULT_RETRY, Backoff, RetryPolicy

CLASS_BACKOFF = Backoff(first_s=10, factor=3, max_s=600, jitter=False)


class PlainJob(Job[None]):
    async def execute(self) -> None:
        return None


class ClassBackoffJob(Job[None]):
    __retry__ = CLASS_BACKOFF

    async def execute(self) -> None:
        return None


def _settings(jobs: dict[str, Any], *job_types: type[Job[Any]]) -> JobSettings:
    ctx = ConfigContext(OmegaConf.create({"jobs": jobs}))
    return JobSettings.from_config(ctx, job_types)


def _policy(settings: JobSettings, job_type: type[Job[Any]]) -> RetryPolicy:
    return settings.of(job_type).retry.policy


def _sources(settings: JobSettings, job_type: type[Job[Any]]) -> dict[str, RetrySource]:
    return dict(settings.of(job_type).retry.sources)


class TestRetryResolution:
    def test_a_job_nothing_configures_gets_the_loom_default(self) -> None:
        resolved = _settings({}, PlainJob).of(PlainJob).retry
        assert resolved.policy == DEFAULT_RETRY
        assert set(resolved.sources.values()) == {RetrySource.LOOM}

    def test_the_class_backoff_replaces_the_default_but_not_its_attempt_limit(self) -> None:
        resolved = _settings({}, ClassBackoffJob).of(ClassBackoffJob).retry
        assert resolved.policy == RetryPolicy(CLASS_BACKOFF, max_attempts=3)
        assert resolved.sources["first_s"] is RetrySource.CLASS
        assert resolved.sources["max_attempts"] is RetrySource.LOOM

    def test_the_defaults_section_wins_over_the_class(self) -> None:
        settings = _settings({"defaults": {"retry": {"first_s": 7}}}, ClassBackoffJob)
        assert _policy(settings, ClassBackoffJob).backoff == Backoff(7, 3, 600, jitter=False)
        sources = _sources(settings, ClassBackoffJob)
        assert sources["first_s"] is RetrySource.DEFAULTS_CONFIG
        assert sources["factor"] is RetrySource.CLASS

    def test_the_job_section_wins_over_the_defaults_section(self) -> None:
        settings = _settings(
            {
                "defaults": {"retry": {"first_s": 7, "max_attempts": 9}},
                "PlainJob": {"retry": {"first_s": 2}},
            },
            PlainJob,
        )
        policy = _policy(settings, PlainJob)
        assert (policy.backoff.first_s, policy.max_attempts) == (2, 9)
        sources = _sources(settings, PlainJob)
        assert sources["first_s"] is RetrySource.JOB_CONFIG
        assert sources["max_attempts"] is RetrySource.DEFAULTS_CONFIG

    def test_a_partial_section_inherits_each_missing_field_from_the_layer_below(self) -> None:
        settings = _settings({"ClassBackoffJob": {"retry": {"max_attempts": 5}}}, ClassBackoffJob)
        assert _policy(settings, ClassBackoffJob) == RetryPolicy(CLASS_BACKOFF, max_attempts=5)

    def test_a_section_without_a_retry_changes_no_source(self) -> None:
        settings = _settings({"PlainJob": {"queue": "slow"}}, PlainJob)
        assert set(_sources(settings, PlainJob).values()) == {RetrySource.LOOM}

    def test_a_yaml_section_can_turn_jitter_off(self) -> None:
        settings = _settings({"defaults": {"retry": {"jitter": False}}}, PlainJob)
        assert _policy(settings, PlainJob).backoff.jitter is False

    def test_the_attempt_limit_never_comes_from_a_class_attribute(self) -> None:
        class AttemptsJob(Job[None]):
            __max_attempts__ = 9

            async def execute(self) -> None:
                return None

        settings = _settings({}, AttemptsJob)
        assert _policy(settings, AttemptsJob).max_attempts == 3

    @pytest.mark.parametrize(
        ("field", "value"),
        [("max_s", 901), ("max_attempts", 0), ("first_s", -1), ("factor", 0.5)],
    )
    def test_an_invalid_field_is_a_config_error_naming_the_section_and_the_field(
        self, field: str, value: float
    ) -> None:
        with pytest.raises(
            ConfigError, match=rf"jobs\.PlainJob\.retry: {field} must|{field} cannot"
        ):
            _settings({"PlainJob": {"retry": {field: value}}}, PlainJob)

    def test_an_unknown_retry_field_is_refused(self) -> None:
        with pytest.raises(ConfigError):
            _settings({"PlainJob": {"retry": {"attempts": 5}}}, PlainJob)

    def test_the_retries_key_is_gone(self) -> None:
        with pytest.raises(ConfigError):
            _settings({"PlainJob": {"retries": 3}}, PlainJob)

    def test_a_job_the_settings_were_not_built_with_has_its_class_policy(self) -> None:
        resolved = JobSettings().of(ClassBackoffJob).retry
        assert resolved.sources["first_s"] is RetrySource.CLASS


class TestRetryDescription:
    def test_a_policy_from_one_layer_names_it_once(self) -> None:
        settings = _settings({}, PlainJob)
        assert settings.describe_retries() == [
            {
                "job": "PlainJob",
                "retry": "first_s=5 factor=2 max_s=300 jitter=True max_attempts=3 (loom default)",
            }
        ]

    def test_a_mixed_policy_names_the_layer_of_each_field_as_text(self) -> None:
        settings = _settings(
            {
                "defaults": {"retry": {"max_attempts": 5}},
                "ClassBackoffJob": {"retry": {"first_s": 7}},
            },
            ClassBackoffJob,
        )
        [line] = settings.describe_retries()
        assert line["retry"] == (
            "first_s=7 (jobs.<Job>.retry), factor=3 (__retry__), max_s=600 (__retry__), "
            "jitter=False (__retry__), max_attempts=5 (jobs.defaults.retry)"
        )
