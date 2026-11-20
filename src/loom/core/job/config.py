"""Transport-neutral per-job configuration overrides."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field, replace
from enum import StrEnum
from typing import Any

from msgspec import structs

from loom.core.config import ConfigContext, ConfigError, ConfigKey
from loom.core.job.job import Job
from loom.core.job.policy import DEFAULT_RETRY, Backoff, Countdown, Redelivery, RetryPolicy
from loom.core.model import LoomFrozenStruct


class RetrySource(StrEnum):
    """A layer a retry field can come from, most specific first."""

    JOB_CONFIG = "jobs.<Job>.retry"
    DEFAULTS_CONFIG = "jobs.defaults.retry"
    CLASS = "__retry__"
    LOOM = "loom default"
    REDRIVE = "RedrivePolicy maxReceiveCount"
    NO_REDELIVERY = "no redelivery"


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
    """A job's effective retry policy and the layer each of its fields came from."""

    policy: RetryPolicy
    sources: Mapping[str, RetrySource]

    @classmethod
    def resolve(
        cls,
        job_type: type[Job[Any]],
        defaults: RetryConfig = _NO_RETRY,
        job: RetryConfig = _NO_RETRY,
    ) -> ResolvedRetry:
        """The loom default, under ``__retry__`` of *job_type*, under *defaults*, under *job*.

        Each field takes the value of the most specific layer that sets it.

        Raises:
            ValueError: When the resulting policy is invalid.
        """
        fields: dict[str, tuple[Any, RetrySource]] = {
            name: (value, RetrySource.LOOM)
            for name, value in (
                *asdict(DEFAULT_RETRY.backoff).items(),
                ("max_attempts", DEFAULT_RETRY.max_attempts),
            )
        }
        if job_type.__retry__ is not None:
            fields |= {
                name: (value, RetrySource.CLASS)
                for name, value in asdict(job_type.__retry__).items()
            }
        for layer, source in (
            (defaults, RetrySource.DEFAULTS_CONFIG),
            (job, RetrySource.JOB_CONFIG),
        ):
            fields |= {
                name: (value, source)
                for name, value in structs.asdict(layer).items()
                if value is not None
            }
        values = {name: value for name, (value, _) in fields.items()}
        max_attempts = values.pop("max_attempts")
        return cls(
            RetryPolicy(Backoff(**values), max_attempts),
            {name: source for name, (_, source) in fields.items()},
        )

    def describe(self, redelivery: Redelivery) -> str:
        """The policy and where it came from: one source when every field shares it.

        Args:
            redelivery: What the transport does with a job that must run again;
                a transport that retries nothing shows one attempt.
        """
        values = {**asdict(self.policy.backoff), "max_attempts": self.policy.max_attempts}
        sources = dict(self.sources)
        if not redelivery.can_retry:
            values["max_attempts"] = 1
            sources["max_attempts"] = RetrySource.NO_REDELIVERY
        origins = set(sources.values())
        if len(origins) == 1:
            return f"{_format_fields(values)} ({origins.pop().value})"
        return ", ".join(
            f"{name}={value} ({sources[name].value})" for name, value in values.items()
        )


def _format_fields(values: Mapping[str, Any]) -> str:
    return " ".join(f"{name}={value}" for name, value in values.items())


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
            ConfigError: When the effective countdown is outside 0 to
                :attr:`~loom.core.job.policy.Countdown.MAX_SECONDS` seconds.
        """
        seconds = job_type.__countdown__ if config.countdown is None else config.countdown
        try:
            countdown = Countdown(seconds).seconds
        except ValueError as exc:
            raise ConfigError(
                f"{job_type.__name__} countdown is {seconds}; "
                f"a countdown is 0 to {Countdown.MAX_SECONDS} seconds"
            ) from exc
        return cls(
            queue=job_type.__queue__ if config.queue is None else config.queue,
            retry=ResolvedRetry.resolve(job_type, defaults.retry, config.retry),
            countdown=countdown,
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
            ConfigError: When a section is present but invalid, a retry layer
                makes a job's backoff invalid, or a job's effective countdown is
                outside 0 to :attr:`~loom.core.job.policy.Countdown.MAX_SECONDS`
                seconds.
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
        """The options of *job_type* in this application.

        Raises:
            ConfigError: When *job_type* is not in this application and its
                countdown is outside 0 to
                :attr:`~loom.core.job.policy.Countdown.MAX_SECONDS` seconds.
        """
        return self.jobs.get(job_type) or JobOptions.resolve(job_type)

    def with_retries(self, retries: Mapping[type[Job[Any]], ResolvedRetry]) -> JobSettings:
        """These settings with the retry policy of each job in *retries* replaced."""
        replaced = {job: replace(self.of(job), retry=retry) for job, retry in retries.items()}
        return replace(self, jobs={**self.jobs, **replaced})

    def describe_retries(
        self, redelivery: Redelivery, jobs: Iterable[type[Job[Any]]]
    ) -> list[dict[str, str]]:
        """Each job's effective retry policy and where it came from, for the start-up log.

        Args:
            redelivery: What the transport does with a job that must run again.
            jobs: The jobs the runner serves.
        """
        return [
            {"job": job_type.__qualname__, "retry": self.of(job_type).retry.describe(redelivery)}
            for job_type in jobs
        ]
