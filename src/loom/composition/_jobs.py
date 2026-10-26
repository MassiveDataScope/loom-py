"""Job service wiring for the composition root."""

from __future__ import annotations

import importlib.util
import warnings
from typing import Any

from loom.core.bootstrap import KernelRuntime
from loom.core.config import ConfigContext, ConfigKey
from loom.core.di.scope import Scope
from loom.core.job.service import InlineJobService, JobService
from loom.core.observability.runtime import ObservabilityRuntime


def build_celery_service(
    ctx: ConfigContext,
    result: KernelRuntime,
    observability_runtime: ObservabilityRuntime | None,
) -> Any | None:
    """Return a ``CeleryJobService`` if the celery extra is installed and configured.

    Returns ``None`` when the ``loom[celery]`` extra is absent or the
    ``celery`` section is, so the caller can fall back to
    :func:`build_inline_service`. A section that fails validation raises.

    Args:
        ctx: Resolved configuration context.
        result: Kernel runtime carrying container, factory and executor.

    Returns:
        ``CeleryJobService`` instance, or ``None``.
    """
    if not ctx.has(ConfigKey.CELERY):
        # Checked before any import: an application without a celery section
        # must not pay for loading Celery. find_spec locates without importing.
        if importlib.util.find_spec("celery") is not None:
            warnings.warn(
                "Celery config section is missing — falling back to InlineJobService. "
                "Add a 'celery' section to your config to suppress this.",
                stacklevel=3,
            )
        return None
    try:
        from loom.celery.config import (  # type: ignore[import-untyped,unused-ignore]
            CeleryConfig as _CC,
        )
        from loom.celery.config import (
            create_celery_app,
        )
        from loom.celery.service import (
            CeleryJobService,  # type: ignore[import-untyped,unused-ignore]
        )

    except ImportError:
        return None
    celery_cfg = ctx.section(ConfigKey.CELERY, _CC)
    return CeleryJobService(
        create_celery_app(celery_cfg),
        metrics=result.metrics,
        factory=result.factory,
        executor=result.executor,
        observability_runtime=observability_runtime,
    )


def build_inline_service(result: KernelRuntime) -> InlineJobService:
    """Return an ``InlineJobService`` backed by the kernel's factory and executor.

    Args:
        result: Kernel runtime carrying factory and executor.

    Returns:
        ``InlineJobService`` instance.
    """
    return InlineJobService(result.factory, result.executor)


def configure_job_service(
    ctx: ConfigContext,
    result: KernelRuntime,
    observability_runtime: ObservabilityRuntime | None,
) -> None:
    """Register a ``JobService`` implementation in the container.

    Registers :class:`~loom.celery.service.CeleryJobService` when a
    ``celery`` config section is present and the ``loom[celery]`` extra is
    installed.  Falls back to
    :class:`~loom.core.job.service.InlineJobService` otherwise — enabling
    local development and tests without a broker.

    The registration uses ``APPLICATION`` scope so the service is created
    once and shared across all requests.

    Args:
        ctx: Resolved configuration context.
        result: Kernel runtime carrying container, factory and executor.
    """
    svc = build_celery_service(ctx, result, observability_runtime) or build_inline_service(result)
    result.container.register(JobService, lambda: svc, scope=Scope.APPLICATION)
