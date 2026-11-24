"""LifecycleObserver bridges for ``loom.prefect``.

This sub-package contains the two observers loom's
:class:`~loom.core.observability.observability.Observability` attaches
when a pipeline runs under Prefect:

- :class:`PrefectObserver` — writes loom's lifecycle records and publishes
  a pipeline-level summary artifact.
- :class:`PrefectTaskRunObserver` — materialises one Prefect ``TaskRun``
  per loom ``Scope.STEP`` event so each step shows up as a row in the
  flow run's UI.

Outside a Prefect flow context the task-run observer does nothing and
:class:`PrefectObserver` writes only loom's records, so both are safe to
register unconditionally.
"""

from loom.prefect.observer._flow import PrefectObserver
from loom.prefect.observer._manifest import ManifestObserver
from loom.prefect.observer._task_run import PrefectTaskRunObserver

__all__ = ["ManifestObserver", "PrefectObserver", "PrefectTaskRunObserver"]
