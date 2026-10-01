from __future__ import annotations

import dataclasses
import re
from collections import Counter
from importlib.resources import files
from types import SimpleNamespace
from typing import Any

import pytest

from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.rls import BootstrapConfig, DatabaseRoles, DatabaseUser
from loom.core.repository.sqlalchemy.rls.bootstrap import QUIET_LOGS, scram_sha256_verifier
from loom.core.repository.sqlalchemy.rls.guard_manifest import (
    GUARD_REVISIONS,
    MIN_COMPATIBLE_GUARD_REVISION,
    preflight_sql,
)
from loom.core.repository.sqlalchemy.rls.integrity import (
    CATALOG_CHECKS,
    access_problems,
    catalog_problems,
    require_revision,
    revision_of,
)
from loom.core.schema_names import SchemaNames

GUARD = files("loom.core.repository.sqlalchemy.rls") / "guard"
LITERAL = re.compile(r"'(?:[^']|'')*'")
MIN_LITERAL = 5
MAX_REPEATS = 2


def _config(**overrides: object) -> BootstrapConfig:
    config = BootstrapConfig(
        schema="s1",
        roles=DatabaseRoles(owner="r_owner", migrator="r_migrator"),
        database_users={"u_read": DatabaseUser(login=True, access="read")},
        names=SchemaNames.derived("s1"),
    )
    return dataclasses.replace(config, **overrides)


def _repeated_literals(text: str) -> dict[str, int]:
    counts = Counter(m for m in LITERAL.findall(text) if len(m) - 2 >= MIN_LITERAL)
    return {literal: count for literal, count in counts.items() if count > MAX_REPEATS}


@pytest.mark.parametrize(
    "filename", sorted(entry.name for entry in GUARD.iterdir() if entry.name.endswith(".sql"))
)
def test_no_quoted_literal_of_five_characters_repeats_three_times_in_a_guard_file(
    filename: str,
) -> None:
    text = (GUARD / filename).read_text(encoding="utf-8")

    assert _repeated_literals(text) == {}


def test_the_literal_counter_finds_a_repetition() -> None:
    text = "SELECT 'bypass', 'bypass', 'bypass', 'four', 'four', 'four'"

    assert _repeated_literals(text) == {"'bypass'": 3}


def test_the_preflight_pins_its_search_path() -> None:
    assert "SET search_path = pg_catalog, pg_temp" in preflight_sql()


def test_every_revision_pins_every_catalog_check() -> None:
    for revision in GUARD_REVISIONS:
        assert set(revision.catalog) == set(CATALOG_CHECKS)


def test_the_minimum_compatible_revision_is_a_released_one() -> None:
    assert MIN_COMPATIBLE_GUARD_REVISION in {revision.number for revision in GUARD_REVISIONS}


def test_a_function_fingerprint_names_its_revision() -> None:
    latest = GUARD_REVISIONS[-1]

    assert revision_of(latest.catalog["functions"]) == latest.number
    assert revision_of("0" * 64) is None


def test_a_guard_below_the_minimum_revision_is_pending() -> None:
    with pytest.raises(ConfigError, match="guard revision pending"):
        require_revision(1, "loom_guard_s1", minimum=2)


def test_an_unknown_guard_is_refused() -> None:
    with pytest.raises(ConfigError, match="not one this release"):
        require_revision(None, "loom_guard_s1", minimum=1)


def test_a_guard_at_the_minimum_revision_is_accepted() -> None:
    assert require_revision(1, "loom_guard_s1", minimum=1) is None


def test_a_catalog_that_differs_is_named_by_check() -> None:
    lines = {check: [] for check in CATALOG_CHECKS}
    lines["constraints"] = ["config.extra"]

    problems = catalog_problems(lines, "g")

    assert "guard.constraints" in {problem.check for problem in problems}


def _function(**overrides: Any) -> Any:
    row = {
        "kind": "function",
        "subject": "protect_scoped_table(tbl regclass)",
        "name": "protect_scoped_table",
        "owner": "postgres",
        "grants": ["o:EXECUTE"],
        "config": ["search_path=pg_catalog, g, pg_temp"],
    }
    return SimpleNamespace(**{**row, **overrides})


def test_a_function_search_path_puts_pg_catalog_first() -> None:
    row = _function(config=["search_path=g, pg_catalog, pg_temp"])

    problems = access_problems([row], "g", "o")

    assert {problem.check for problem in problems} == {"guard.function_config"}


def test_execute_granted_to_a_foreign_role_is_reported() -> None:
    row = _function(grants=["o:EXECUTE", "r:EXECUTE"])

    problems = access_problems([row], "g", "o")

    assert {problem.check for problem in problems} == {"guard.function_grants"}


def test_the_password_function_pins_log_statement() -> None:
    row = _function(name="set_password_verifier", grants=[])

    problems = access_problems([row], "g", "o")

    assert {problem.check for problem in problems} == {"guard.function_config"}


def test_scram_iterations_default_to_the_postgres_default() -> None:
    assert _config().scram_iterations == 4096


def test_scram_iterations_below_the_postgres_default_are_refused() -> None:
    config = _config(scram_iterations=1000)

    with pytest.raises(ValueError, match="scram_iterations"):
        config.validated()


def test_the_verifier_carries_the_configured_iterations() -> None:
    verifier = scram_sha256_verifier("pw", salt=b"0" * 16, iterations=8192)

    assert verifier.startswith("SCRAM-SHA-256$8192:")


def test_revoking_public_is_opt_in() -> None:
    assert _config().revoke_public is False


def test_the_document_names_every_config_column() -> None:
    document = _config().document()

    assert set(document) == {
        "app_schema",
        "owner_role",
        "migrator_role",
        "readers_role",
        "writers_role",
        "version_table",
        "data_version_table",
        "users",
        "revoke_public",
    }


def test_the_bootstrap_quiets_statement_logging_locally() -> None:
    for setting in ("log_statement", "log_min_duration_statement", "log_parameter_max_length"):
        assert f"set_config('{setting}'" in QUIET_LOGS
    assert QUIET_LOGS.count(", true)") == 3
