"""Celery-layer constants: callback task names and queues."""

from __future__ import annotations

from loom.core.job.envelope import TASK_JOB_PREFIX as TASK_JOB_PREFIX

TASK_CALLBACK_PREFIX = "loom.callback"
TASK_CALLBACK_ERROR_PREFIX = "loom.callback_error"

QUEUE_DEFAULT = "default"
