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
    SCHEMA_DEFINERS,
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
    "assert_scoped_schema": "7feffb6cfa34f2375f5d2692277e295679345ac6f42832246170a117245027e2",
    "protect_scoped_table": "3f04c320c167a931292fce3f78d5f725cf7de84a3c75a885625ba67103264c8d",
    "assert_audited_schema": "5a3d689c214e1d2a715d6bc054a33bd8ab1c99180e19b24cd87fbb3ff7f5b153",
    "configure": "b420f7c2d9df55bf928229dd9dd519aef842556e44302a0cd91001dd58cf04e1",
    "owner_functions": "5cdd55977a17b55f11b3e5221a8e760615953c2eb64bb66572b197df84bb232c",
    "on_ddl_end": "fafefa3944b9de8443ef6f777c761944fa977c19fe784116d173f225788db7c2",
    "on_sql_drop": "c60f897a16ae58b7da5f93cc2efbb40b5f5d6c61413b828c8a73dbf93e2b4af1",
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
        "acl_written",
        "apply_audit_reading",
        "audit_log_readers",
        "audit_reader_source",
        "ddl_wakes_guard",
        "drop_wakes_guard",
        "duplicate_scopes",
        "guarded_relations",
        "harmless_drops",
        "harmless_tags",
        "lineage",
        "may_expose",
        "namespace_tags",
        "normal_scope",
        "reader_name",
        "refuse_audit_log_views",
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


def test_ddl_defers_the_full_assertion_and_the_walk_to_commit() -> None:
    """T12c: the event triggers enter the hatch; its deferred check asserts once, at COMMIT."""
    for name in ("on_ddl_end", "on_sql_drop"):
        body = _REPLACED[name]
        assert "PERFORM enter_hatch();" in body, name
        assert "assert_scoped_schema" not in body, name
        assert "refuse_audit_log_views" not in body, name
    assert "PERFORM refuse_audit_log_views();" in _REPLACED["assert_audited_schema"]


def test_guard_schema_ddl_is_still_refused_at_the_statement() -> None:
    """The one immediate check: cheap, exact, and before anything else."""
    for name in ("on_ddl_end", "on_sql_drop"):
        body = _REPLACED[name]
        assert body.index("forbid_guard_ddl") < body.index("hatch_open()"), name


def test_only_ddl_that_may_expose_the_log_runs_the_walk_again() -> None:
    """T12c R2: views, rules and their options; never a refresh."""
    exposure = _CREATED["may_expose"]
    assert "'view', 'materialized view', 'rule'" in exposure
    assert "REFRESH MATERIALIZED VIEW" in exposure
    assert "may_expose(" in _CREATED["ddl_wakes_guard"]


def test_the_harmless_schemaless_commands_are_the_approved_list() -> None:
    """T12c R1.5: any other command naming no schema wakes every guard (fail closed)."""
    tags = set(re.findall(r"'([A-Z ]+)'", _CREATED["harmless_tags"]))
    assert tags == {
        "ALTER DEFAULT PRIVILEGES",
        "CREATE PUBLICATION",
        "ALTER PUBLICATION",
        "CREATE SUBSCRIPTION",
        "ALTER SUBSCRIPTION",
        "CREATE LANGUAGE",
        "CREATE CAST",
        "CREATE ACCESS METHOD",
        "CREATE FOREIGN DATA WRAPPER",
        "ALTER FOREIGN DATA WRAPPER",
        "CREATE SERVER",
        "ALTER SERVER",
        "CREATE USER MAPPING",
        "ALTER USER MAPPING",
    }
    assert set(re.findall(r"'([A-Z ]+)'", _CREATED["namespace_tags"])) == {
        "CREATE SCHEMA",
        "ALTER SCHEMA",
        "COMMENT",
        "SECURITY LABEL",
    }


def test_the_revision_refuses_an_existing_view_as_soon_as_it_can() -> None:
    """Review L1: an upgrade over a view fails naming it, before anything else is replaced."""
    created = _CREATED["refuse_audit_log_views"]
    following = _REVISION[_REVISION.index(created) + len(created) :]

    assert following.lstrip().startswith("SELECT refuse_audit_log_views();")


def test_the_view_check_and_on_ddl_end_skip_a_guard_without_configuration() -> None:
    """Info: before ``configure`` the log's name is unknown; ``format('%I', NULL)`` raises."""
    early = "IF app_schema() IS NULL THEN RETURN; END IF;"

    assert early in _CREATED["refuse_audit_log_views"]
    assert "DECLARE app name := app_schema();" in _REPLACED["on_ddl_end"]
    assert "IF app IS NULL OR hatch_open() THEN RETURN; END IF;" in _REPLACED["on_ddl_end"]


def test_the_view_check_lists_what_audit_log_readers_returns() -> None:
    """Review M1: views and rules come from one function, which a test compares with Python."""
    assert "audit_log_readers()" in _CREATED["refuse_audit_log_views"]
    readers = _CREATED["audit_log_readers"]
    assert "rulename <> '_RETURN'" in readers
    assert "security_invoker" in readers


def test_the_assertion_refuses_a_foreign_trigger_on_the_log() -> None:
    """Review M2: only the guard's two immutability triggers may sit on the log tree."""
    assert "NOT g.tgisinternal" in _REPLACED["assert_audited_schema"]


def test_the_schema_definers_are_the_functions_the_guard_creates_in_the_store_schema() -> None:
    """The verify allow-list names what the guard text installs as ``<app schema>.<name>``."""
    text = "".join((_GUARD / name).read_text() for name in ("0001.sql", "0002.sql", "0003.sql"))
    text += _REVISION
    qualified = set(re.findall(r"%I\.(\w+)\(", text))
    guard_functions = set(_definitions(text)) | set(_previous())

    assert qualified - guard_functions == {name.partition("(")[0] for name in SCHEMA_DEFINERS}
