"""Sections with a single reader refuse a key they do not declare.

A misspelt key, written in a file or set through a ``LOOM__`` variable, is a
validation error instead of a setting silently ignored.
"""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.celery.config import CeleryConfig
from loom.core.config.convert import convert_config
from loom.core.observability.config import (
    LogObservabilityConfig,
    OtelObservabilityConfig,
    PrometheusObservabilityConfig,
)
from loom.rest.auth import JwtAuthConfig
from loom.rest.cors import CorsConfig
from loom.rest.fastapi.auto import _RestAuthConfig, _RestConfig


@pytest.mark.parametrize(
    ("struct", "document", "typo"),
    [
        (_RestConfig, {"docs_ulr": None}, "docs_ulr"),
        (_RestAuthConfig, {"custm": True}, "custm"),
        (
            JwtAuthConfig,
            {"secret_path": "/k", "algorithms": ["HS256"], "audiance": "a"},
            "audiance",
        ),
        (CorsConfig, {"allow_orgins": ["https://a.example"]}, "allow_orgins"),
        (LogObservabilityConfig, {"enabld": True}, "enabld"),
        (OtelObservabilityConfig, {"export_log": True}, "export_log"),
        (PrometheusObservabilityConfig, {"enabeld": True}, "enabeld"),
        (CeleryConfig, {"broker_url": "memory://", "broker_urll": "x"}, "broker_urll"),
    ],
)
def test_a_misspelt_key_is_refused(struct: type[Any], document: dict[str, Any], typo: str) -> None:
    with pytest.raises(msgspec.ValidationError, match=typo):
        convert_config(document, struct, strict=True)
