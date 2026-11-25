"""Fixtures of the logging tests."""

from __future__ import annotations

import logging

import pytest

from tests.helpers.loggers import CapturingHandler


@pytest.fixture
def pristine_root_logger() -> logging.Logger:
    """The root logger; the root conftest restores it after each test."""
    return logging.getLogger()


@pytest.fixture
def handler(rendering: CapturingHandler, pristine_root_logger: logging.Logger) -> CapturingHandler:
    """The handler of ``rendering`` on a root logger that lets every level through."""
    pristine_root_logger.setLevel(logging.DEBUG)
    return rendering
