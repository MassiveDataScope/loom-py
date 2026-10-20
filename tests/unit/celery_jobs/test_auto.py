"""Unit tests for loom.celery.auto.create_app."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

from loom.celery.auto import create_app
from loom.core.job.job import Job


class _DummyJob(Job[int]):
    def execute(self) -> int:
        return 1


def test_create_app_delegates_to_bootstrap_worker() -> None:
    expected = MagicMock()
    result = SimpleNamespace(celery_app=expected)
    with patch("loom.celery.auto.bootstrap_worker", return_value=result) as mock_bootstrap:
        app = create_app("config/worker.yaml", jobs=[_DummyJob])
    assert app is expected
    mock_bootstrap.assert_called_once_with(
        "config/worker.yaml",
        config_dir=None,
        environment=None,
        jobs=[_DummyJob],
        callbacks=(),
        modules=(),
        metrics=None,
        resolvers=(),
    )


def test_create_app_allows_discovery_without_explicit_jobs() -> None:
    expected = MagicMock()
    result = SimpleNamespace(celery_app=expected)
    with patch("loom.celery.auto.bootstrap_worker", return_value=result) as mock_bootstrap:
        app = create_app("config/worker.yaml")
    assert app is expected
    mock_bootstrap.assert_called_once_with(
        "config/worker.yaml",
        config_dir=None,
        environment=None,
        jobs=(),
        callbacks=(),
        modules=(),
        metrics=None,
        resolvers=(),
    )


def test_create_app_passes_optional_arguments() -> None:
    expected = MagicMock()
    result = SimpleNamespace(celery_app=expected)
    module = MagicMock()
    callback: type[Any] = type("Callback", (), {})
    metrics = MagicMock()

    with patch("loom.celery.auto.bootstrap_worker", return_value=result) as mock_bootstrap:
        app = create_app(
            "base.yaml",
            "worker.yaml",
            jobs=[_DummyJob],
            callbacks=[callback],
            modules=[module],
            metrics=metrics,
        )

    assert app is expected
    mock_bootstrap.assert_called_once_with(
        "base.yaml",
        "worker.yaml",
        config_dir=None,
        environment=None,
        jobs=[_DummyJob],
        callbacks=[callback],
        modules=[module],
        metrics=metrics,
        resolvers=(),
    )


def test_create_app_forwards_resolvers() -> None:
    result = SimpleNamespace(celery_app=MagicMock())
    resolver = MagicMock()

    with patch("loom.celery.auto.bootstrap_worker", return_value=result) as mock_bootstrap:
        create_app("config/worker.yaml", jobs=[_DummyJob], resolvers=[resolver])

    assert mock_bootstrap.call_args.kwargs["resolvers"] == [resolver]


def test_create_app_forwards_the_config_directory_and_environment() -> None:
    result = SimpleNamespace(celery_app=MagicMock())

    with patch("loom.celery.auto.bootstrap_worker", return_value=result) as mock_bootstrap:
        create_app(config_dir="config", environment="staging", jobs=[_DummyJob])

    kwargs = mock_bootstrap.call_args.kwargs
    assert (kwargs["config_dir"], kwargs["environment"]) == ("config", "staging")
