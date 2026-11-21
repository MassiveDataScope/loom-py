from __future__ import annotations

from importlib.resources import files

from loom.stores.sqlalchemy.rls.guard_manifest import (
    GUARD_REVISIONS,
    PREFLIGHT_FILE,
    PREFLIGHT_SHA256,
    digest,
    preflight_sql,
)

GUARD = files("loom.stores.sqlalchemy.rls") / "guard"


class TestGuardRelease:
    def test_every_released_revision_matches_its_pinned_digest(self) -> None:
        for revision in GUARD_REVISIONS:
            text = (GUARD / revision.filename).read_text(encoding="utf-8")

            assert digest(text) == revision.sha256, revision.filename
            assert revision.sql() == text

    def test_the_preflight_matches_its_pinned_digest(self) -> None:
        text = (GUARD / PREFLIGHT_FILE).read_text(encoding="utf-8")

        assert digest(text) == PREFLIGHT_SHA256
        assert preflight_sql() == text

    def test_revisions_are_numbered_from_one_without_gaps(self) -> None:
        numbers = [revision.number for revision in GUARD_REVISIONS]

        assert numbers == list(range(1, len(numbers) + 1))

    def test_every_packaged_revision_file_is_released(self) -> None:
        packaged = {
            entry.name
            for entry in GUARD.iterdir()
            if entry.name.endswith(".sql") and entry.name != PREFLIGHT_FILE
        }

        assert packaged == {revision.filename for revision in GUARD_REVISIONS}
