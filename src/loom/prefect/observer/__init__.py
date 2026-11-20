"""LifecycleObserver bridges for ``loom.prefect``.

This sub-package contains the two observers loom's
:class:`~loom.core.observability.observability.Observability` attaches
when a pipeline runs under Prefect:

- :class:`PrefectObserver` — forwards events to the Prefect run logger
  and publishes a pipeline-level summary artifact.
- :class:`PrefectTaskRunObserver` — materialises one Prefect ``TaskRun``
  per loom ``Scope.STEP`` event so each step shows up as a row in the
  flow run's UI.

Both observers are no-ops outside a Prefect flow context, so they are
safe to register unconditionally.
"""

from loom.prefect.observer._flow import PrefectObserver
from loom.prefect.observer._manifest import ManifestObserver
from loom.prefect.observer._task_run import PrefectTaskRunObserver

__all__ = ["ManifestObserver", "PrefectObserver", "PrefectTaskRunObserver"]
