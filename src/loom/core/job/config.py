"""Transport-neutral per-job configuration overrides."""

from __future__ import annotations

from typing import Any

from loom.core.job.job import Job
from loom.core.model import LoomFrozenStruct


class JobConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Per-job routing and execution overrides applied at bootstrap.

    Fields set to ``None`` leave the corresponding ClassVar on the
    :class:`~loom.core.job.job.Job` subclass unchanged.

    Attributes:
        queue: Target Celery queue name.
        retries: Maximum number of automatic retries on failure.
        countdown: Delay in seconds before first execution.
        timeout: Soft time limit in seconds.
        priority: Task priority (broker-dependent interpretation).

    Example::

        job_cfg = section(omegaconf_obj, "jobs.RecalcPricesJob", JobConfig)
        apply_job_config(RecalcPricesJob, job_cfg)
    """

    queue: str | None = None
    retries: int | None = None
    countdown: int | None = None
    timeout: int | None = None
    priority: int | None = None


# Mapping of JobConfig field name → Job ClassVar name.
# Extending this map is the only change needed when a new ClassVar is added.
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
    class — sibling Job types are not affected.

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
