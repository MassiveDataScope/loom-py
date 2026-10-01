from __future__ import annotations

import re

import pytest

from loom.core.repository.sqlalchemy.rls import (
    BootstrapConfig,
    DatabaseRoles,
    DatabaseUser,
    render_bootstrap,
)
from loom.core.repository.sqlalchemy.rls.bootstrap import (
    MIN_SERVER_VERSION_NUM,
    password_statements,
    scram_sha256_verifier,
)


def _config(**overrides: object) -> BootstrapConfig:
    base: dict[str, object] = {
        "schema": "s1",
        "roles": DatabaseRoles(owner="r_owner", migrator="r_migrator"),
        "database_users": {
            "u_read": DatabaseUser(login=True, access="read"),
            "u_write": DatabaseUser(login=True, access="write"),
            "u_bypass": DatabaseUser(login=True, access="bypass"),
        },
    }
    base.update(overrides)
    return BootstrapConfig(**base)  # type: ignore[arg-type]


@pytest.fixture(scope="module")
def sql() -> str:
    return render_bootstrap(_config())


GUARD_OBJECTS = (
    "loom_guard_s1.scoped_table",
    "loom_guard_s1.scoped_policy",
    "loom_guard_s1.term(",
    "loom_guard_s1.deny_owner_dml()",
    "loom_guard_s1.protect_scoped_table(",
    "loom_guard_s1.unprotect_scoped_table(",
    "loom_guard_s1.assert_scoped_schema()",
    "loom_guard_s1.on_ddl_end()",
    "loom_guard_s1.on_sql_drop()",
    "EVENT TRIGGER loom_guard_s1_ddl",
    "EVENT TRIGGER loom_guard_s1_drop",
)


@pytest.mark.parametrize("obj", GUARD_OBJECTS)
def test_every_guard_object_is_rendered_for_the_schema(sql: str, obj: str) -> None:
    assert obj in sql


def test_the_product_names_are_rendered_and_no_placeholder_survives(sql: str) -> None:
    for name in ("s1", "r_owner", "r_migrator", "u_read", "u_write", "u_bypass"):
        assert name in sql
    assert re.search(r"\{[A-Z_]+\}", sql) is None


def test_no_version_suffix_and_no_version_table(sql: str) -> None:
    assert re.search(r"_v\d", sql) is None
    assert "guard_version" not in sql
    assert "loom_guard_s1.version" not in sql


def test_no_password_and_no_getenv_reach_the_rendered_sql(sql: str) -> None:
    assert "PASSWORD" not in sql.upper()
    assert "\\getenv" not in sql


def test_the_server_version_is_checked_first(sql: str) -> None:
    assert MIN_SERVER_VERSION_NUM == 140000
    assert sql.index("server_version_num") < sql.index("CREATE SCHEMA")
    assert "140000" in sql


def test_groups_are_created_and_users_join_them_by_access(sql: str) -> None:
    assert "s1_readers" in sql
    assert "s1_writers" in sql
    assert re.search(r"GRANT s1_readers TO u_read\b", sql)
    assert re.search(r"GRANT s1_readers, s1_writers TO u_write\b", sql)
    assert not re.search(r"GRANT s1_(readers|writers)(, s1_writers)? TO u_bypass\b", sql)


def test_the_migrator_is_a_no_inherit_member_of_the_owner(sql: str) -> None:
    assert re.search(r"GRANT r_owner TO r_migrator;", sql)
    assert "('r_migrator', true, false, false)" in sql
    assert re.search(r"ALTER ROLE r_migrator SET role = r_owner", sql)


def test_role_attributes_are_compared_before_reuse(sql: str) -> None:
    assert "rolbypassrls" in sql
    assert "rolsuper" in sql
    assert "already exists with different attributes" in sql


def test_only_the_owner_reaches_the_guard_schema_and_its_two_entry_points(sql: str) -> None:
    assert re.search(r"GRANT USAGE ON SCHEMA loom_guard_s1 TO r_owner;", sql)
    grant = r"GRANT EXECUTE ON FUNCTION loom_guard_s1\.{}_scoped_table TO r_owner;"
    assert re.search(grant.format("protect"), sql)
    assert re.search(grant.format("unprotect"), sql)
    for other in ("s1_readers", "s1_writers", "u_bypass", "u_read", "u_write"):
        assert not re.search(rf"ON SCHEMA loom_guard_s1 TO {other}\b", sql)
        assert not re.search(rf"ON FUNCTION loom_guard_s1\.\w+ TO {other}\b", sql)


def test_the_event_trigger_privilege_is_checked_with_a_named_error(sql: str) -> None:
    assert "rds_superuser" in sql
    assert "event trigger" in sql.lower()


def test_bypass_users_get_default_privileges_and_the_idempotent_grant_on_all(sql: str) -> None:
    defaults = "ALTER DEFAULT PRIVILEGES FOR ROLE r_owner IN SCHEMA s1 GRANT "
    assert defaults + "SELECT, INSERT, UPDATE, DELETE ON TABLES TO u_bypass" in sql
    assert defaults + "USAGE ON SEQUENCES TO u_bypass" in sql
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA s1 TO u_bypass" in sql
    assert "GRANT USAGE ON ALL SEQUENCES IN SCHEMA s1 TO u_bypass" in sql
    assert sql.index("ON ALL TABLES IN SCHEMA s1 TO u_bypass") < sql.index("alembic_version")
    assert "REVOKE ALL ON TABLE s1.alembic_version FROM u_bypass" in sql


def test_every_bypass_user_is_whitelisted_in_the_assertion() -> None:
    config = _config(
        database_users={
            "u_write": DatabaseUser(login=True, access="write"),
            "ops_a": DatabaseUser(login=True, access="bypass"),
            "ops_b": DatabaseUser(login=True, access="bypass"),
        }
    )

    rendered = render_bootstrap(config)

    assert "'ops_a'" in rendered
    assert "'ops_b'" in rendered


def test_public_is_revoked_from_the_application_schema_by_default(sql: str) -> None:
    assert "REVOKE ALL ON SCHEMA public FROM PUBLIC" in sql
    assert "REVOKE ALL ON SCHEMA public FROM PUBLIC" not in render_bootstrap(
        _config(revoke_public=False)
    )


@pytest.mark.parametrize(
    "bad",
    [
        {"schema": "my-schema"},
        {"schema": "1st"},
        {"roles": DatabaseRoles(owner="own;er", migrator="r_migrator")},
        {"database_users": {"drop table": DatabaseUser(login=True, access="read")}},
    ],
)
def test_bad_identifiers_are_rejected_before_rendering(bad: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="identifier"):
        render_bootstrap(_config(**bad))


def test_the_scram_verifier_has_the_postgres_shape_and_hides_the_password() -> None:
    verifier = scram_sha256_verifier("s3cret", salt=b"0123456789abcdef", iterations=4096)

    head, salt_part, keys = verifier.split("$")
    assert head == "SCRAM-SHA-256"
    assert salt_part.startswith("4096:")
    assert len(keys.split(":")) == 2
    assert "s3cret" not in verifier


def test_the_same_password_and_salt_always_yield_the_same_verifier() -> None:
    a = scram_sha256_verifier("pw", salt=b"0123456789abcdef", iterations=4096)
    b = scram_sha256_verifier("pw", salt=b"0123456789abcdef", iterations=4096)

    assert a == b


def test_password_statements_carry_verifiers_never_cleartext() -> None:
    statements = password_statements({"u_read": "plain-text-secret", "u_bypass": "another"})

    assert len(statements) == 2
    for statement in statements:
        assert statement.startswith("ALTER ROLE ")
        assert "SCRAM-SHA-256$" in statement
        assert "plain-text-secret" not in statement
        assert "another" not in statement


def test_template_comments_mention_no_placeholder() -> None:
    from importlib.resources import files

    template = (
        files("loom.core.repository.sqlalchemy.rls") / "templates" / "bootstrap.sql"
    ).read_text()
    comment_lines = [line for line in template.splitlines() if line.lstrip().startswith("--")]

    assert [line for line in comment_lines if "{" in line] == []


def test_every_role_that_creates_or_reads_lands_in_the_application_schema(sql: str) -> None:
    for role in ("r_owner", "r_migrator", "u_read", "u_write", "u_bypass"):
        assert f"ALTER ROLE {role} SET search_path = s1;" in sql
