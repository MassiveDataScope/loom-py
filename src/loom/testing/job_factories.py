"""Registered factories for the job types a field rule alone cannot build.

Importing ``make`` or ``SampleFactory`` from :mod:`loom.testing` registers them, so
``make(JobDelivery)`` and any type nesting a delivery, an envelope or an outcome
build with no declaration.
"""

from __future__ import annotations

from typing import Any

from polyfactory.fields import Use

from loom.core.job.delivery import JobDelivery
from loom.core.job.envelope import JobEnvelope, wire_job_name
from loom.core.job.outcome import Abandoned, Failed, Succeeded
from loom.testing.data import SampleFactory
from loom.testing.jobs import NoopJob


class JobEnvelopeFactory(SampleFactory[JobEnvelope]):
    """An envelope that names :class:`~loom.testing.jobs.NoopJob` and carries nothing else."""

    __set_as_default_factory_for_type__ = True

    job = Use(lambda: wire_job_name(NoopJob.__qualname__))
    payload = Use(dict[str, Any])
    params = Use(lambda: None)
    trace_id = Use(lambda: None)
    identity = Use(lambda: None)
    task_id = Use(lambda: "task-1")


class JobDeliveryFactory(SampleFactory[JobDelivery]):
    """A first delivery of :class:`~loom.testing.jobs.NoopJob`, with no time limit."""

    __set_as_default_factory_for_type__ = True

    job_type = Use(lambda: NoopJob)
    attempt = Use(lambda: 1)
    postponements = Use(lambda: 0)
    timeout_s = Use(lambda: None)


class SucceededFactory(SampleFactory[Succeeded]):
    """A job that finished with a result nothing asserts on."""

    __set_as_default_factory_for_type__ = True

    result = Use(lambda: "done")


class FailedFactory(SampleFactory[Failed]):
    """An attempt that failed with a runtime error."""

    __set_as_default_factory_for_type__ = True

    error = Use(lambda: RuntimeError("boom"))


class AbandonedFactory(SampleFactory[Abandoned]):
    """A job given up without an error of its own."""

    __set_as_default_factory_for_type__ = True

    error = Use(lambda: None)
