"""Fixtures shared by the Prefect unit tests."""

from __future__ import annotations

import textwrap
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from prefect.context import FlowRunContext
from prefect.settings import PREFECT_LOGGING_TO_API_ENABLED, temporary_settings


@pytest.fixture
def write_yaml(tmp_path: Path) -> Callable[[str, str], Path]:
    """Write a YAML document under ``tmp_path`` and return its path."""

    def write(name: str, body: str) -> Path:
        path = tmp_path / name
        path.write_text(textwrap.dedent(body), encoding="utf-8")
        return path

    return write


@pytest.fixture
def prefect_run_context() -> Iterator[None]:
    """Open a flow-run context whose run logger does not ship logs to the API."""
    with (
        temporary_settings({PREFECT_LOGGING_TO_API_ENABLED: False}),
        FlowRunContext.model_construct(flow=None, flow_run=None),
    ):
        yield
