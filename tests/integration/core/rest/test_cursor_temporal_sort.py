"""Cursor pages sorted by a ``time`` or ``date`` column walk every row through REST."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from loom.rest.fastapi.auto import create_app

_APP = '''\
"""Fixture app listing slots sorted by a time or a date column."""

from __future__ import annotations

from datetime import date, time

from loom.core.model import BaseModel, ColumnField
from loom.rest.model import RestInterface


class Slot(BaseModel):
    __tablename__ = "temporal_sort_slots"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    starts_at: time = ColumnField()
    held_on: date = ColumnField()


class SlotInterface(RestInterface[Slot]):
    prefix = "/slots"
    auto = True
    sortable = ("starts_at", "held_on")
'''

_ROWS = [
    {"starts_at": "09:30:00", "held_on": "2026-10-03"},
    {"starts_at": "08:15:00", "held_on": "2026-10-01"},
    {"starts_at": "11:00:00", "held_on": "2026-10-02"},
]


def _config(tmp_path: Path) -> str:
    module = f"loom_temporal_sort_{tmp_path.name.replace('-', '_')}"
    (tmp_path / f"{module}.py").write_text(_APP, encoding="utf-8")
    config = tmp_path / "app.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "app": {
                    "name": "temporal-sort",
                    "code_path": ".",
                    "discovery": {
                        "mode": "interfaces",
                        "interfaces": {"modules": [module], "warn_recommended": False},
                    },
                },
                "database": {"url": f"sqlite+aiosqlite:///{tmp_path / 'slots.sqlite'}"},
                "persistence": {"cursors": {"signing": "ephemeral"}},
            }
        ),
        encoding="utf-8",
    )
    return str(config)


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        ("starts_at", ["08:15:00", "09:30:00", "11:00:00"]),
        ("held_on", ["2026-10-01", "2026-10-02", "2026-10-03"]),
    ],
)
def test_a_cursor_page_sorted_by_a_temporal_column_reaches_the_next_page(
    tmp_path: Path, field: str, expected: list[str]
) -> None:
    app = create_app(_config(tmp_path), environment="development")
    page = f"/slots/?pagination=cursor&limit=2&sort={field}&direction=ASC"

    with TestClient(app, raise_server_exceptions=False) as client:
        for row in _ROWS:
            assert client.post("/slots/", json=row).status_code == 201
        first = client.get(page)
        second = client.get(f"{page}&after={first.json()['nextCursor']}")

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    key = {"starts_at": "startsAt", "held_on": "heldOn"}[field]
    walked = [item[key] for item in first.json()["items"] + second.json()["items"]]
    assert walked == expected
    assert second.json()["nextCursor"] is None
