"""Doubles and fixtures for tests that drive the Celery worker lifecycle signals."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from queue import Empty
from typing import Any, cast

import pytest
from celery import Celery  # type: ignore[import-untyped]
from celery.utils.dispatch import Signal  # type: ignore[import-untyped]


class WorkerDouble:
    """The ``worker_init`` sender of a worker: the controller, its pool not yet resolved.

    Defaults to ``--pool=solo`` and Celery's default ``fork`` start method.
    """

    def __init__(self, pool: object = "solo", start_method: str = "fork") -> None:
        self.pool_cls = pool
        self.pool_start_method = start_method


def isolated_signals(*signals: Signal, clear: bool = False) -> Callable[[], Iterator[None]]:
    """An autouse fixture saving *signals*' receivers and restoring them after each test.

    Bind it to a module-level name so pytest collects it. With *clear* each
    test starts with no receiver connected.
    """

    @pytest.fixture(autouse=True)
    def isolated() -> Iterator[None]:
        saved = [list(cast(list[Any], signal.receivers)) for signal in signals]
        if clear:
            for signal in signals:
                cast(list[Any], signal.receivers)[:] = []
        yield
        for signal, receivers in zip(signals, saved, strict=True):
            cast(list[Any], signal.receivers)[:] = receivers

    return isolated


@dataclass(frozen=True)
class BrokerMessage:
    """One task message as the in-memory broker holds it.

    Attributes:
        task: The task name.
        task_id: The id the message was sent with.
        queue: The queue it was routed to.
        kwargs: The keyword arguments the task receives.
        countdown: Whole seconds the message waits, from its ETA.
        priority: The broker priority, when one was sent.
        soft_time_limit: The soft time limit in seconds, when one was sent.
        callbacks: The success links the message carries, as signatures.
    """

    task: str
    task_id: str
    queue: str
    kwargs: dict[str, Any]
    countdown: int
    priority: int | None
    soft_time_limit: float | None
    callbacks: list[dict[str, Any]]


class CeleryBroker:
    """A Celery application on the in-memory broker, and the messages sent through it.

    Args:
        queues: The queues to read; each is emptied when the broker is built,
            as the in-memory transport is shared by every application of the process.

    Attributes:
        app: The application a transport sends through.
    """

    def __init__(self, queues: Iterable[str]) -> None:
        self.app = Celery(broker="memory://", backend="cache+memory://")
        self._queues = tuple(queues)
        self._left: list[BrokerMessage] = []
        with self.app.connection_for_read() as connection:
            for queue in self._queues:
                connection.default_channel.queue_purge(queue)

    def sent(self) -> list[BrokerMessage]:
        """Every message sent so far, oldest first within each queue."""
        with self.app.connection_for_read() as connection:
            for queue in self._queues:
                with connection.SimpleQueue(queue) as messages:
                    while (message := self._take(messages)) is not None:
                        self._left.append(self._decoded(message, queue))
        return list(self._left)

    @staticmethod
    def _take(messages: Any) -> Any:
        try:
            message = messages.get(block=False)
        except Empty:
            return None
        message.ack()
        return message

    @staticmethod
    def _decoded(message: Any, queue: str) -> BrokerMessage:
        headers = message.headers
        eta = headers["eta"]
        waiting = (
            0 if eta is None else (datetime.fromisoformat(eta) - datetime.now(UTC)).total_seconds()
        )
        return BrokerMessage(
            task=headers["task"],
            task_id=headers["id"],
            queue=queue,
            kwargs=message.payload[1],
            countdown=max(round(waiting), 0),
            priority=message.properties.get("priority"),
            soft_time_limit=headers["timelimit"][1],
            callbacks=message.payload[2]["callbacks"] or [],
        )
