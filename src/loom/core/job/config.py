"""Transport-neutral per-job configuration overrides."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from loom.core.config import ConfigContext, ConfigError, ConfigKey
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

    Example::

        job_cfg = ctx.section("jobs.RecalcPricesJob", JobConfig)
        JobSettings({RecalcPricesJob: job_cfg}).queue(RecalcPricesJob)
    """

    queue: str | None = None
    retries: int | None = None
    countdown: int | None = None
    timeout: int | None = None
    priority: int | None = None


# Mapping of JobConfig field name → Job ClassVar name.
_JOB_CONFIG_FIELDS: dict[str, str] = {
    "queue": "__queue__",
    "retries": "__retries__",
    "countdown": "__countdown__",
    "timeout": "__timeout__",
    "priority": "__priority__",
}


def apply_job_config(job_type: type[Job[Any]], cfg: JobConfig) -> None:
    """Apply ``cfg`` overrides to ``job_type``'s ClassVars in-place.

    Only fields that are not ``None`` in ``cfg`` are applied.  This makes
    ``apply_job_config`` idempotent: calling it with an empty ``JobConfig``
    leaves the ClassVars unchanged.

    The override targets the concrete subclass, never the ``Job`` base
    class — sibling Job types are not affected. The ClassVars are shared by
    every application in the process: the composition reads the sections
    into a :class:`JobSettings` instead and never calls this.

    Args:
        job_type: Concrete ``Job`` subclass to configure.
        cfg: Override values.  ``None`` fields are skipped.

    Example::

        apply_job_config(SendEmailJob, JobConfig(queue="email", retries=3))
        assert SendEmailJob.__queue__ == "email"
    """
    for field_name, class_var in _JOB_CONFIG_FIELDS.items():
        value = getattr(cfg, field_name)
        if value is not None:
            setattr(job_type, class_var, value)


_EMPTY = JobConfig()


def read_job_config(ctx: ConfigContext, job_type: type[Job[Any]]) -> JobConfig:
    """Return the ``jobs.<JobTypeName>`` section, empty when it is absent or unreadable."""
    with suppress(ConfigError):
        return ctx.section(f"{ConfigKey.JOBS}.{job_type.__name__}", JobConfig)
    return JobConfig()


@dataclass(frozen=True)
class JobSettings:
    """Each job's settings in one application: its ``jobs.<Job>`` section over its ClassVars.

    The composition builds one per application, so two applications in one
    process never see each other's sections and the ClassVars stay as
    declared. A job without a section, or a field the section leaves
    ``None``, keeps its ClassVar.

    Args:
        configs: The section of each configured job.
    """

    configs: Mapping[type[Job[Any]], JobConfig] = field(default_factory=dict)

    @classmethod
    def from_config(cls, ctx: ConfigContext, jobs: Iterable[type[Job[Any]]]) -> JobSettings:
        """Read the ``jobs.<Job>`` section of each of *jobs* from *ctx*."""
        return cls({job_type: read_job_config(ctx, job_type) for job_type in jobs})

    def queue(self, job_type: type[Job[Any]]) -> str:
        """The queue *job_type* is sent to."""
        configured = self._config(job_type).queue
        return job_type.__queue__ if configured is None else configured

    def retries(self, job_type: type[Job[Any]]) -> int:
        """How many times a failed *job_type* is retried."""
        configured = self._config(job_type).retries
        return job_type.__retries__ if configured is None else configured

    def countdown(self, job_type: type[Job[Any]]) -> int:
        """Seconds before *job_type* first runs."""
        configured = self._config(job_type).countdown
        return job_type.__countdown__ if configured is None else configured

    def timeout(self, job_type: type[Job[Any]]) -> int | None:
        """Soft time limit of *job_type* in seconds; ``None`` means none."""
        configured = self._config(job_type).timeout
        return job_type.__timeout__ if configured is None else configured

    def priority(self, job_type: type[Job[Any]]) -> int:
        """Priority of *job_type* on its broker."""
        configured = self._config(job_type).priority
        return job_type.__priority__ if configured is None else configured

    def _config(self, job_type: type[Job[Any]]) -> JobConfig:
        return self.configs.get(job_type, _EMPTY)
