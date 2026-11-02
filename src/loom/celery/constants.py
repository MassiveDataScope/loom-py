"""Celery-layer constants: task names, queues, and worker manifest keys."""

from __future__ import annotations

from loom.core.job.manifest import WorkerManifestAttr as WorkerManifestAttr

TASK_JOB_PREFIX = "loom.job"
TASK_CALLBACK_PREFIX = "loom.callback"
TASK_CALLBACK_ERROR_PREFIX = "loom.callback_error"

QUEUE_DEFAULT = "default"
