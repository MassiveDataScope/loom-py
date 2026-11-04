"""Transport-neutral per-job configuration overrides."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from loom.core.config import ConfigContext, ConfigKey
from loom.core.job.job import Job
from loom.core.model import LoomFrozenStruct


class JobConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Per-job routing and execution overrides applied at bootstrap.

    Fields set to ``None`` keep the corresponding ClassVar of the
    :class:`~loom.core.job.job.Job` subclass.

    Attributes:
        queue: Target Celery queue name.
        retries: Maximum number of automatic retries on failure.
        countdown: Delay in seconds before first execution.
        timeout: Soft time limit in seconds.
        priority: Task priority (broker-dependent interpretation).
    """

    queue: str | None = None
    retries: int | None = None
    countdown: int | None = None
    timeout: int | None = None
    priority: int | None = None


_EMPTY = JobConfig()


@dataclass(frozen=True)
class JobOptions:
    """A job's effective queue, retries, countdown, soft time limit and priority."""

    queue: str
    retries: int
    countdown: int
    timeout: int | None
    priority: int

    @classmethod
    def resolve(cls, job_type: type[Job[Any]], config: JobConfig = _EMPTY) -> JobOptions:
        """*config* over the ClassVars of *job_type*: a ``None`` field keeps its ClassVar."""
        return cls(
            queue=job_type.__queue__ if config.queue is None else config.queue,
            retries=job_type.__retries__ if config.retries is None else config.retries,
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
            ConfigError: When a section is present but invalid.
        """
        return cls(
            {
                job_type: JobOptions.resolve(
                    job_type,
                    ctx.section_or_default(
                        f"{ConfigKey.JOBS}.{job_type.__name__}", JobConfig, _EMPTY
                    ),
                )
                for job_type in jobs
            }
        )

    def of(self, job_type: type[Job[Any]]) -> JobOptions:
        """The options of *job_type* in this application."""
        return self.jobs.get(job_type) or JobOptions.resolve(job_type)
