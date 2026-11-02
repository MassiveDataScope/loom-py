"""Doubles and fixtures for tests that drive the Celery worker lifecycle signals."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, cast

import pytest
from celery.utils.dispatch import Signal  # type: ignore[import-untyped]


class WorkerDouble:
    """The ``worker_init`` sender of a worker: the controller, its pool not yet resolved.

    Defaults to ``--pool=solo``.
    """

    def __init__(self, pool: object = "solo") -> None:
        self.pool_cls = pool


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
