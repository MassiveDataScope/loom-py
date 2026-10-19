"""``loom.logger.W001``: console log lines in a deployed environment.

A deployed environment renders JSON by default, so console lines there come
only from an explicit renderer, top-level or on a handler; log collectors
parse JSON, not console lines.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.checks import run_deploy_checks
from loom.core.config import ConfigContext
from loom.core.logger.checks import CONSOLE_RENDERER_CHECK

_LOG = "log"


def _ids(observability: dict[str, Any] | None) -> list[str]:
    raw: dict[str, Any] = {} if observability is None else {"observability": observability}
    report = run_deploy_checks(ConfigContext.from_dict(raw))
    return [result.id for result in report.results if result.id == CONSOLE_RENDERER_CHECK]


@pytest.mark.parametrize(
    "observability",
    [
        None,
        {_LOG: {"config": {"level": "INFO"}}},
        {_LOG: {"config": {"renderer": "json"}}},
        {_LOG: {"enabled": False, "config": {"renderer": "console"}}},
    ],
)
def test_json_or_default_rendering_passes(observability: dict[str, Any] | None) -> None:
    assert _ids(observability) == []


@pytest.mark.parametrize(
    "logger_config",
    [
        {"renderer": "console"},
        {"renderer": "CONSOLE"},
        {"handlers": [{"type": "stream", "renderer": "console"}]},
    ],
)
def test_an_explicit_console_renderer_warns(logger_config: dict[str, Any]) -> None:
    assert _ids({_LOG: {"config": logger_config}}) == [CONSOLE_RENDERER_CHECK]


def test_a_placeholder_elsewhere_in_the_logger_config_is_not_resolved() -> None:
    logger_config = {"renderer": "console", "level": "${secrets:/logging/level}"}

    assert _ids({_LOG: {"config": logger_config}}) == [CONSOLE_RENDERER_CHECK]


def test_a_renderer_from_a_placeholder_is_not_resolved_nor_flagged() -> None:
    assert _ids({_LOG: {"config": {"renderer": "${oc.env:UNSET_LOG_RENDERER}"}}}) == []
