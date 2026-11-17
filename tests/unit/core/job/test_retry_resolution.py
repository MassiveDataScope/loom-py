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


class TestRetryResolution:
    def test_a_job_nothing_configures_gets_the_loom_default(self) -> None:
        resolved = _settings({}, PlainJob).of(PlainJob).retry
        assert (resolved.policy, resolved.source) == (DEFAULT_RETRY, RetrySource.LOOM)

    def test_the_class_backoff_replaces_the_default_but_not_its_attempt_limit(self) -> None:
        resolved = _settings({}, ClassBackoffJob).of(ClassBackoffJob).retry
        assert resolved.policy == RetryPolicy(CLASS_BACKOFF, max_attempts=3)
        assert resolved.source is RetrySource.CLASS

    def test_the_defaults_section_wins_over_the_class(self) -> None:
        settings = _settings({"defaults": {"retry": {"first_s": 7}}}, ClassBackoffJob)
        resolved = settings.of(ClassBackoffJob).retry
        assert resolved.policy.backoff == Backoff(7, 3, 600, jitter=False)
        assert resolved.source is RetrySource.DEFAULTS_CONFIG

    def test_the_job_section_wins_over_the_defaults_section(self) -> None:
        settings = _settings(
            {
                "defaults": {"retry": {"first_s": 7, "max_attempts": 9}},
                "PlainJob": {"retry": {"first_s": 2}},
            },
            PlainJob,
        )
        resolved = settings.of(PlainJob).retry
        assert resolved.policy.backoff.first_s == 2
        assert resolved.policy.max_attempts == 9
        assert resolved.source is RetrySource.JOB_CONFIG

    def test_a_partial_section_inherits_each_missing_field_from_the_layer_below(self) -> None:
        settings = _settings({"ClassBackoffJob": {"retry": {"max_attempts": 5}}}, ClassBackoffJob)
        assert _policy(settings, ClassBackoffJob) == RetryPolicy(CLASS_BACKOFF, max_attempts=5)

    def test_a_section_without_a_retry_leaves_the_source_of_the_layer_below(self) -> None:
        settings = _settings({"PlainJob": {"queue": "slow"}}, PlainJob)
        assert settings.of(PlainJob).retry.source is RetrySource.LOOM

    def test_a_yaml_section_can_turn_jitter_off(self) -> None:
        settings = _settings({"defaults": {"retry": {"jitter": False}}}, PlainJob)
        assert _policy(settings, PlainJob).backoff.jitter is False

    def test_an_invalid_resulting_backoff_is_a_config_error_naming_the_section(self) -> None:
        with pytest.raises(ConfigError, match=r"jobs\.PlainJob\.retry"):
            _settings({"PlainJob": {"retry": {"max_s": 901}}}, PlainJob)

    def test_an_unknown_retry_field_is_refused(self) -> None:
        with pytest.raises(ConfigError):
            _settings({"PlainJob": {"retry": {"attempts": 5}}}, PlainJob)

    def test_the_retries_key_is_gone(self) -> None:
        with pytest.raises(ConfigError):
            _settings({"PlainJob": {"retries": 3}}, PlainJob)

    def test_a_job_the_settings_were_not_built_with_has_its_class_policy(self) -> None:
        resolved = JobSettings().of(ClassBackoffJob).retry
        assert resolved.source is RetrySource.CLASS


class TestRetryDescription:
    def test_describes_each_job_with_its_effective_policy_and_source(self) -> None:
        settings = _settings(
            {"PlainJob": {"retry": {"max_attempts": 4}}}, PlainJob, ClassBackoffJob
        )
        assert settings.describe_retries() == [
            {
                "job": "PlainJob",
                "first_s": 5,
                "factor": 2,
                "max_s": 300,
                "jitter": True,
                "max_attempts": 4,
                "source": "jobs.<Job>.retry",
            },
            {
                "job": "ClassBackoffJob",
                "first_s": 10,
                "factor": 3,
                "max_s": 600,
                "jitter": False,
                "max_attempts": 3,
                "source": "__retry__",
            },
        ]
