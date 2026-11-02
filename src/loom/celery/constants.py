"""Celery-layer constants: task names, queues, and worker manifest keys."""

from __future__ import annotations

from loom.core.discovery.worker_manifest import WorkerManifestAttr as WorkerManifestAttr
from loom.core.job.envelope import TASK_JOB_PREFIX as TASK_JOB_PREFIX

TASK_CALLBACK_PREFIX = "loom.callback"
TASK_CALLBACK_ERROR_PREFIX = "loom.callback_error"

QUEUE_DEFAULT = "default"
