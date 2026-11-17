"""Transport-neutral per-job configuration overrides."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from loom.core.config import ConfigContext, ConfigError, ConfigKey
from loom.core.job.job import Job
from loom.core.job.policy import DEFAULT_RETRY, Backoff, RetryPolicy
from loom.core.model import LoomFrozenStruct


class RetrySource(StrEnum):
    """The most specific layer that set a job's retry policy, most specific first."""

    JOB_CONFIG = "jobs.<Job>.retry"
    DEFAULTS_CONFIG = "jobs.defaults.retry"
    CLASS = "__retry__"
    LOOM = "loom default"


class RetryConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """A ``retry`` section; a field left unset keeps the value of the layer below.

    Attributes:
        first_s: Ceiling of the delay after the first failure.
        factor: Growth of the ceiling for each further attempt.
        max_s: Ceiling of every delay, at most 900.
        jitter: Whether a delay is drawn from zero to its ceiling.
        max_attempts: Attempts before a failure is final.
    """

    first_s: int | None = None
    factor: float | None = None
    max_s: int | None = None
    jitter: bool | None = None
    max_attempts: int | None = None

    def over(self, policy: RetryPolicy) -> RetryPolicy:
        """*policy* with every field this section sets.

        Raises:
            ValueError: When the resulting backoff is invalid.
        """
        backoff = policy.backoff
        return RetryPolicy(
            Backoff(
                first_s=backoff.first_s if self.first_s is None else self.first_s,
                factor=backoff.factor if self.factor is None else self.factor,
                max_s=backoff.max_s if self.max_s is None else self.max_s,
                jitter=backoff.jitter if self.jitter is None else self.jitter,
            ),
            policy.max_attempts if self.max_attempts is None else self.max_attempts,
        )

    def sets_anything(self) -> bool:
        """Whether any field is set."""
        return self != _NO_RETRY


_NO_RETRY = RetryConfig()


class JobDefaults(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """The ``jobs.defaults`` section: what every job without its own section gets.

    Attributes:
        retry: The retry layer every job inherits from.
    """

    retry: RetryConfig = _NO_RETRY


class JobConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Per-job routing and execution overrides applied at bootstrap.

    Fields set to ``None`` keep the corresponding ClassVar of the
    :class:`~loom.core.job.job.Job` subclass.

    Attributes:
        queue: Target Celery queue name.
        retry: The job's retry layer over ``jobs.defaults.retry``.
        countdown: Delay in seconds before first execution.
        timeout: Soft time limit in seconds.
        priority: Task priority (broker-dependent interpretation).
    """

    queue: str | None = None
    retry: RetryConfig = _NO_RETRY
    countdown: int | None = None
    timeout: int | None = None
    priority: int | None = None


_EMPTY = JobConfig()
_NO_DEFAULTS = JobDefaults()


@dataclass(frozen=True)
class ResolvedRetry:
    """A job's effective retry policy and the most specific layer that set it."""

    policy: RetryPolicy
    source: RetrySource

    @classmethod
    def resolve(
        cls,
        job_type: type[Job[Any]],
        defaults: RetryConfig = _NO_RETRY,
        job: RetryConfig = _NO_RETRY,
    ) -> ResolvedRetry:
        """The loom default, under ``__retry__`` of *job_type*, under *defaults*, under *job*.

        Raises:
            ValueError: When a layer makes the backoff invalid.
        """
        policy, source = DEFAULT_RETRY, RetrySource.LOOM
        if job_type.__retry__ is not None:
            policy, source = RetryPolicy(job_type.__retry__, policy.max_attempts), RetrySource.CLASS
        for layer, layer_source in (
            (defaults, RetrySource.DEFAULTS_CONFIG),
            (job, RetrySource.JOB_CONFIG),
        ):
            if layer.sets_anything():
                policy, source = layer.over(policy), layer_source
        return cls(policy, source)


@dataclass(frozen=True)
class JobOptions:
    """A job's effective queue, retry policy, countdown, soft time limit and priority."""

    queue: str
    retry: ResolvedRetry
    countdown: int
    timeout: int | None
    priority: int

    @classmethod
    def resolve(
        cls,
        job_type: type[Job[Any]],
        config: JobConfig = _EMPTY,
        defaults: JobDefaults = _NO_DEFAULTS,
    ) -> JobOptions:
        """*config* over the ClassVars of *job_type*: a ``None`` field keeps its ClassVar.

        Raises:
            ValueError: When a retry layer makes the backoff invalid.
        """
        return cls(
            queue=job_type.__queue__ if config.queue is None else config.queue,
            retry=ResolvedRetry.resolve(job_type, defaults.retry, config.retry),
            countdown=job_type.__countdown__ if config.countdown is None else config.countdown,
            timeout=job_type.__timeout__ if config.timeout is None else config.timeout,
            priority=job_type.__priority__ if config.priority is None else config.priority,
        )


@dataclass(frozen=True)
class JobSettings:
    """Each job's options in one application: its ``jobs.<Job>`` section over its ClassVars.

    The composition builds one per application, so two applications in one
    process never see each other's sections and the ClassVars stay as
    declared. A job it was not built with has its ClassVars.
    """

    jobs: Mapping[type[Job[Any]], JobOptions] = field(default_factory=dict)

    @classmethod
    def from_config(cls, ctx: ConfigContext, jobs: Iterable[type[Job[Any]]]) -> JobSettings:
        """Resolve each of *jobs* against its ``jobs.<Job>`` section in *ctx*.

        Raises:
            ConfigError: When a section is present but invalid, or a retry
                layer makes a job's backoff invalid.
        """
        defaults = ctx.section_or_default(f"{ConfigKey.JOBS}.defaults", JobDefaults, _NO_DEFAULTS)
        resolved: dict[type[Job[Any]], JobOptions] = {}
        for job_type in jobs:
            section = f"{ConfigKey.JOBS}.{job_type.__name__}"
            config = ctx.section_or_default(section, JobConfig, _EMPTY)
            try:
                resolved[job_type] = JobOptions.resolve(job_type, config, defaults)
            except ValueError as exc:
                raise ConfigError(f"{section}.retry: {exc}") from exc
        return cls(resolved)

    def of(self, job_type: type[Job[Any]]) -> JobOptions:
        """The options of *job_type* in this application."""
        return self.jobs.get(job_type) or JobOptions.resolve(job_type)

    def describe_retries(self) -> list[dict[str, Any]]:
        """Each job's effective retry policy and its source, for the start-up log."""
        return [
            {
                "job": job_type.__qualname__,
                "first_s": options.retry.policy.backoff.first_s,
                "factor": options.retry.policy.backoff.factor,
                "max_s": options.retry.policy.backoff.max_s,
                "jitter": options.retry.policy.backoff.jitter,
                "max_attempts": options.retry.policy.max_attempts,
                "source": options.retry.source,
            }
            for job_type, options in self.jobs.items()
        ]
