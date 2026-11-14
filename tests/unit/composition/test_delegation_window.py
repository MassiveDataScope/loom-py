"""``identity.max_delegation_seconds``: read at composition, then in effect (T09a)."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest

from loom.core.config import ConfigError
from loom.core.identity import DEFAULT_MAX_DELEGATION_SECONDS, Identity, captured_delegation
from loom.rest.fastapi.auto import create_app
from tests.helpers.delegation import acting_as, assert_new_delegation
from tests.unit.rest._fixture_app import write_project

_ALICE = Identity(subject="acme-alice", mechanism="test")


@pytest.mark.usefixtures("window")
class TestAComposedApplication:
    """Composing sets the process window; the ``window`` fixture gives the previous one back."""

    @pytest.mark.parametrize(
        ("identity", "seconds"),
        [({"max_delegation_seconds": 5}, 5.0), (None, 3600.0)],
        ids=["configured", "one-hour-by-default"],
    )
    def test_a_request_s_delegation_lasts_the_configured_window(
        self, tmp_path: Path, identity: dict[str, Any] | None, seconds: float
    ) -> None:
        create_app(write_project(tmp_path, identity=identity))

        before = time.time()
        with acting_as(_ALICE):
            _, deadline = captured_delegation()

        assert_new_delegation(deadline, before, seconds)
        assert DEFAULT_MAX_DELEGATION_SECONDS == 3600.0

    @pytest.mark.parametrize("seconds", [0, "abc"], ids=["zero", "not-numeric"])
    def test_an_invalid_window_is_refused_before_serving(
        self, tmp_path: Path, seconds: object
    ) -> None:
        with pytest.raises(ConfigError, match="identity.max_delegation_seconds"):
            create_app(write_project(tmp_path, identity={"max_delegation_seconds": seconds}))
