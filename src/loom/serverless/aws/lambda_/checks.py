"""The job an event trigger runs, refused at startup and by ``loom check --deploy`` alike."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from loom.core.config import ConfigContext, ConfigError
from loom.core.discovery.worker import discovered_jobs, worker_discovery
from loom.core.use_case.markers import _InputMarker
from loom.serverless.aws.lambda_.functions import EventTrigger, JobsTrigger, declared_lambda

if TYPE_CHECKING:
    from loom.core.job.job import Job


def trigger_job(
    name: str, trigger: EventTrigger, jobs: Mapping[str, type[Job[Any]]]
) -> type[Job[Any]]:
    """The job of *jobs*, by qualified name, the event trigger of function *name* runs.

    Raises:
        ConfigError: When no job of *jobs* is called so, or its ``execute``
            declares no ``Input()`` command or requires another parameter,
            which an event cannot give.
    """
    where = f"lambda.functions.{name}.trigger.job"
    job_type = jobs.get(trigger.job)
    if job_type is None:
        raise ConfigError(f"{where} names {trigger.job!r}, which is no discovered Job")
    has_input, required = _execute_parameters(job_type)
    if not has_input:
        raise ConfigError(f"{where}: {trigger.job}.execute declares no Input() command")
    if required:
        raise ConfigError(
            f"{where}: {trigger.job}.execute requires {', '.join(required)}, "
            "which an event cannot give"
        )
    return job_type


def _execute_parameters(job_type: type[Job[Any]]) -> tuple[bool, list[str]]:
    """Whether ``execute`` declares an ``Input()`` command, and its other required parameters."""
    has_input, required = False, []
    for parameter in inspect.signature(job_type.execute).parameters.values():
        if parameter.name == "self" or parameter.kind in (
            parameter.VAR_POSITIONAL,
            parameter.VAR_KEYWORD,
        ):
            continue
        if isinstance(parameter.default, _InputMarker):
            has_input = True
        elif parameter.default is parameter.empty:
            required.append(parameter.name)
    return has_input, required


def check_triggers(ctx: ConfigContext) -> None:
    """Refuse, as startup would, a misdeclared ``lambda`` section or an event trigger's job.

    The jobs are discovered from ``app.discovery`` as written: nothing resolves.

    Raises:
        ConfigError: When ``lambda.functions`` is misdeclared, the jobs
            cannot be discovered, or :func:`trigger_job` refuses one.
    """
    functions = declared_lambda(ctx).functions
    events = {
        name: function.trigger
        for name, function in functions.items()
        if isinstance(function.trigger, EventTrigger)
    }
    consumes_jobs = any(isinstance(f.trigger, JobsTrigger) for f in functions.values())
    if not events and not consumes_jobs:
        return
    # A jobs function runs what discovery finds: a misdeclared app.discovery fails here too.
    jobs = _discovered_jobs(ctx)
    for name, trigger in events.items():
        trigger_job(name, trigger, jobs)


def _discovered_jobs(ctx: ConfigContext) -> dict[str, type[Job[Any]]]:
    """The jobs ``app.discovery``, as written, discovers, by qualified name.

    Raises:
        ConfigError: When they cannot be discovered.
    """
    try:
        discovery = worker_discovery(ctx.raw("app.discovery", {}))
        return {job.__qualname__: job for job in discovered_jobs(discovery)}
    except (ImportError, RuntimeError, ValueError) as exc:
        raise ConfigError(f"the jobs of the event triggers cannot be discovered: {exc}") from exc
