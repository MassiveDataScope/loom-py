"""Celery-layer constants: callback task names and queues."""

from __future__ import annotations

from typing import Any

from loom.core.job.callback import CallbackOutcome
from loom.core.job.envelope import TASK_JOB_PREFIX as TASK_JOB_PREFIX

TASK_CALLBACK_PREFIX = "loom.callback"
TASK_CALLBACK_ERROR_PREFIX = "loom.callback_error"

QUEUE_DEFAULT = "default"


def callback_task_name(callback_type: type[Any], outcome: CallbackOutcome) -> str:
    """Return the name of the task calling *callback_type*'s hook for *outcome*.

    Args:
        callback_type: The callback class.
        outcome: The outcome whose hook the task calls.

    Returns:
        ``loom.callback.<qualname>`` for a success, ``loom.callback_error.<qualname>``
        for a failure.
    """
    if outcome is CallbackOutcome.SUCCESS:
        return f"{TASK_CALLBACK_PREFIX}.{callback_type.__qualname__}"
    return f"{TASK_CALLBACK_ERROR_PREFIX}.{callback_type.__qualname__}"
