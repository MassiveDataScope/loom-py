"""Revision 4 adds the audit reader and changes earlier functions only by reviewed deltas.

Each ``CREATE OR REPLACE FUNCTION`` in ``0004.sql`` is diffed against its last
earlier definition and the digest of that diff is pinned, as for revision 3: a
failure prints the diff. NFR-002: no line revision 4 adds or removes touches
the write path (the trigger, ``write_audit_row``), the log's ACL or its indexes.
"""

from __future__ import annotations

import difflib
import hashlib
import re
from pathlib import Path

import pytest

from loom.core.repository.sqlalchemy.rls.guard_manifest import (
    AUDIT_GUARD_REVISION,
    GUARD_REVISIONS,
    OWNER_FUNCTIONS,
    READ_AUDIT_LOG_ARGUMENTS,
    READING_GUARD_REVISION,
    required_guard_revision,
)

_GUARD = Path(__file__).parents[6] / "src/loom/core/repository/sqlalchemy/rls/guard"
_HEADER = re.compile(r"^CREATE (OR REPLACE )?FUNCTION (\w+)\(", re.MULTILINE)
_REVISION = (_GUARD / "0004.sql").read_text()
# The write path, the log ACL and the log indexes (NFR-002, FR-004).
_WRITE_PATH = re.compile(
    r"write_audit_row|loom_audit_record|audit_log_\w*idx|audit_log_pkey|CREATE TRIGGER|"
    r"apply_audit_acl|restrict_audit_log|make_immutable|\bINDEX\b",
    re.IGNORECASE,
)


def _definitions(sql: str) -> dict[str, tuple[bool, str]]:
    """Map each function name to (replaces, body text) in one revision file."""
    found: dict[str, tuple[bool, str]] = {}
    starts = list(_HEADER.finditer(sql))
    for match, following in zip(starts, [*starts[1:], None], strict=True):
        end = following.start() if following else len(sql)
        body = sql[match.start() : end]
        body = body[: body.find("$$;") + 3] if "$$;" in body else body
        found[match.group(2)] = (bool(match.group(1)), body)
    return found


def _previous() -> dict[str, str]:
    earlier: dict[str, str] = {}
    for name in ("0001.sql", "0002.sql", "0003.sql"):
        earlier.update({k: v for k, (_, v) in _definitions((_GUARD / name).read_text()).items()})
    return earlier


_DEFINED = _definitions(_REVISION)
_REPLACED = {name: body for name, (replaces, body) in _DEFINED.items() if replaces}
_CREATED = {name: body for name, (replaces, body) in _DEFINED.items() if not replaces}

EXPECTED_DIFFS = {
    "assert_scoped_schema": "87f8a7f783d092a9e1bed9e838648564b868942ea39ded99760643eb0dbbdb52",
    "protect_scoped_table": "3f04c320c167a931292fce3f78d5f725cf7de84a3c75a885625ba67103264c8d",
    "assert_audited_schema": "bd34090f06f1057f77982206f8833c1fced8cbd5473db715a3919a2d91f65358",
    "configure": "b420f7c2d9df55bf928229dd9dd519aef842556e44302a0cd91001dd58cf04e1",
    "owner_functions": "5cdd55977a17b55f11b3e5221a8e760615953c2eb64bb66572b197df84bb232c",
}


def _delta(name: str) -> list[str]:
    before = _previous()[name].splitlines()
    after = _REPLACED[name].splitlines()
    lines = difflib.unified_diff(before, after, lineterm="", n=0)
    return [line for line in lines if not line.startswith(("---", "+++"))]


def _new_text() -> str:
    """Revision 4 without the unchanged lines of the functions it replaces."""
    text = _REVISION
    for body in _REPLACED.values():
        text = text.replace(body, "")
    changed = (line for name in _REPLACED for line in _delta(name) if line[:1] in "+-")
    return text + "\n".join(changed)


def test_the_replaced_functions_are_exactly_the_reviewed_set() -> None:
    assert sorted(_REPLACED) == sorted(EXPECTED_DIFFS)


def test_every_replaced_function_replaces_an_earlier_definition() -> None:
    assert set(_REPLACED) <= set(_previous())


def test_the_created_functions_are_the_reader_source_the_owner_step_and_its_checks() -> None:
    assert sorted(_CREATED) == [
        "apply_audit_reading",
        "audit_reader_source",
        "duplicate_scopes",
        "normal_scope",
        "scope_of",
        "scope_set",
        "tree_mismatch",
        "unbounded_tables",
    ]
    assert not set(_CREATED) & set(_previous())


@pytest.mark.parametrize("name", sorted(EXPECTED_DIFFS))
def test_a_replaced_function_changes_only_by_its_reviewed_delta(name: str) -> None:
    delta = "\n".join(_delta(name))
    digest = hashlib.sha256(delta.encode()).hexdigest()

    review = f"{name} changed; review the delta and pin {digest}:\n{delta}"
    assert digest == EXPECTED_DIFFS[name], review


def test_revision_four_does_not_touch_the_write_path_the_log_acl_or_its_indexes() -> None:
    assert _WRITE_PATH.findall(_new_text()) == []


def test_the_write_path_pattern_finds_a_mention() -> None:
    assert _WRITE_PATH.findall("PERFORM restrict_audit_log(c); -- write_audit_row") == [
        "restrict_audit_log",
        "write_audit_row",
    ]


def test_the_owner_function_list_matches_the_manifest() -> None:
    listed = re.search(r"SELECT ARRAY\[(.*?)\]::name\[\]", _REPLACED["owner_functions"], re.S)

    assert listed is not None
    assert set(re.findall(r"'(\w+)'", listed.group(1))) == OWNER_FUNCTIONS
    created = {"audit_reader_source", "apply_audit_reading", "unbounded_tables"}
    helpers = {"scope_of", "duplicate_scopes", "normal_scope", "scope_set", "tree_mismatch"}
    assert created | helpers <= OWNER_FUNCTIONS


def test_the_reader_source_resolves_column_conflicts_to_columns() -> None:
    source = _CREATED["audit_reader_source"]

    assert "$src$\n#variable_conflict use_column\n" in source


def test_the_reader_is_installed_and_pinned_with_the_released_arguments() -> None:
    assert _REVISION.count(READ_AUDIT_LOG_ARGUMENTS) == 2
    assert READ_AUDIT_LOG_ARGUMENTS in _CREATED["apply_audit_reading"]
    assert READ_AUDIT_LOG_ARGUMENTS in _REPLACED["assert_audited_schema"]


def test_reading_needs_revision_four() -> None:
    assert GUARD_REVISIONS[-1].number == READING_GUARD_REVISION == 4
    assert required_guard_revision(False) is None
    assert required_guard_revision(True) == AUDIT_GUARD_REVISION
    assert required_guard_revision(False, reading=True) == READING_GUARD_REVISION
    assert required_guard_revision(True, reading=True) == READING_GUARD_REVISION
