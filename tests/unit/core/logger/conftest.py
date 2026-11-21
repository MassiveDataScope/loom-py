"""Fixtures of the logging tests."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest

from tests.helpers.loggers import CapturingHandler


@pytest.fixture
def pristine_root_logger() -> logging.Logger:
    """The root logger; the root conftest restores it after each test."""
    return logging.getLogger()


@pytest.fixture
def handler(pristine_root_logger: logging.Logger) -> Iterator[CapturingHandler]:
    """A handler on the root logger that keeps what reaches it."""
    capturing = CapturingHandler()
    pristine_root_logger.addHandler(capturing)
    pristine_root_logger.setLevel(logging.DEBUG)
    yield capturing
    pristine_root_logger.removeHandler(capturing)
