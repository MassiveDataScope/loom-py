"""``create_app`` wires the ``cache:`` section (AC5 of spec 009).

With the section, the ``@cached`` repository of the fixture project is served
wrapped: a second read of the same row never reaches the database.  Without
it, every read does and boot warns once; either way shutdown closes each
distinct gateway the deployment opened.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from loom.core.cache.event import CacheEvent
from loom.core.cache.gateway import CacheGateway
from loom.rest.fastapi.auto import create_app
from tests.helpers.loggers import rendered
from tests.unit.core.cache._doubles import MEMORY_BACKEND, SERIALIZED_BACKEND
from tests.unit.rest._fixture_app import CACHED_READS, CACHED_RECORDS_PREFIX, write_project

_SINGLE_ALIAS: dict[str, Any] = {"aiocache_config": {"default": dict(SERIALIZED_BACKEND)}}
_TWO_ALIASES: dict[str, Any] = {
    "aiocache_alias": "data",
    "counter_alias": "counters",
    "aiocache_config": {"data": dict(SERIALIZED_BACKEND), "counters": dict(MEMORY_BACKEND)},
}


@pytest.fixture(autouse=True)
def _fresh_read_counter() -> Iterator[None]:
    CACHED_READS.reset()
    yield
    CACHED_READS.reset()


@pytest.fixture
def close_calls(monkeypatch: pytest.MonkeyPatch) -> list[CacheGateway]:
    calls: list[CacheGateway] = []

    async def _close(self: CacheGateway) -> None:
        calls.append(self)

    monkeypatch.setattr(CacheGateway, "close", _close)
    return calls


def _get_twice(client: TestClient) -> tuple[dict[str, Any], dict[str, Any]]:
    created = client.post(f"{CACHED_RECORDS_PREFIX}/", json={"name": "one"})
    assert created.status_code == 201, created.text
    CACHED_READS.reset()
    url = f"{CACHED_RECORDS_PREFIX}/{created.json()['id']}"
    first, second = client.get(url), client.get(url)
    assert first.status_code == second.status_code == 200
    return first.json(), second.json()


class TestReadsThroughTheCache:
    def test_a_second_read_never_reaches_the_database_with_the_section(
        self, tmp_path: Path
    ) -> None:
        app = create_app(write_project(tmp_path, cache=_SINGLE_ALIAS, with_cached_model=True))

        with TestClient(app) as client:
            first, second = _get_twice(client)

        assert first == second
        assert CACHED_READS.calls == 1

    def test_every_read_reaches_the_database_and_boot_warns_without_the_section(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="loom.core.cache.wiring"):
            app = create_app(write_project(tmp_path, with_cached_model=True))

        with TestClient(app) as client:
            first, second = _get_twice(client)

        assert first == second
        assert CACHED_READS.calls == 2
        warnings = [
            record
            for record in caplog.records
            if CacheEvent.REPOSITORY_NOT_CONFIGURED in record.getMessage()
        ]
        assert len(warnings) == 1
        assert warnings[0].levelno == logging.WARNING
        assert "CachedRecordRepository" in rendered(warnings[0])


class TestShutdownClosesTheGateways:
    def test_a_single_alias_is_closed_once(
        self, tmp_path: Path, close_calls: list[CacheGateway]
    ) -> None:
        app = create_app(write_project(tmp_path, cache=_SINGLE_ALIAS, with_cached_model=True))

        with TestClient(app):
            assert close_calls == []

        assert len(close_calls) == 1

    def test_a_counter_alias_is_closed_beside_the_data_one(
        self, tmp_path: Path, close_calls: list[CacheGateway]
    ) -> None:
        app = create_app(write_project(tmp_path, cache=_TWO_ALIASES, with_cached_model=True))

        with TestClient(app):
            assert close_calls == []

        assert len(close_calls) == 2
        assert close_calls[0] is not close_calls[1]

    def test_nothing_is_closed_without_the_section(
        self, tmp_path: Path, close_calls: list[CacheGateway]
    ) -> None:
        app = create_app(write_project(tmp_path, with_cached_model=True))

        with TestClient(app):
            pass

        assert close_calls == []
