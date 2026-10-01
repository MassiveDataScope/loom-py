from __future__ import annotations

from importlib.resources import files

from loom.core.repository.sqlalchemy.rls.guard_manifest import (
    GUARD_REVISIONS,
    PREFLIGHT_FILE,
    PREFLIGHT_SHA256,
    REQUIRED_GUARD_REVISION,
    digest,
    preflight_sql,
)

GUARD = files("loom.core.repository.sqlalchemy.rls") / "guard"


def test_every_released_revision_matches_its_pinned_digest() -> None:
    for revision in GUARD_REVISIONS:
        text = (GUARD / revision.filename).read_text(encoding="utf-8")

        assert digest(text) == revision.sha256, revision.filename
        assert revision.sql() == text


def test_the_preflight_matches_its_pinned_digest() -> None:
    text = (GUARD / PREFLIGHT_FILE).read_text(encoding="utf-8")

    assert digest(text) == PREFLIGHT_SHA256
    assert preflight_sql() == text


def test_revisions_are_numbered_from_one_without_gaps() -> None:
    numbers = [revision.number for revision in GUARD_REVISIONS]

    assert numbers == list(range(1, len(numbers) + 1))
    assert numbers[-1] == REQUIRED_GUARD_REVISION


def test_every_packaged_revision_file_is_released() -> None:
    packaged = {
        entry.name
        for entry in GUARD.iterdir()
        if entry.name.endswith(".sql") and entry.name != PREFLIGHT_FILE
    }

    assert packaged == {revision.filename for revision in GUARD_REVISIONS}
