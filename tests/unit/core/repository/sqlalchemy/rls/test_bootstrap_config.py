from __future__ import annotations

import base64
import dataclasses

import pytest

from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.rls import (
    MIN_SERVER_VERSION_NUM,
    BootstrapConfig,
    DatabaseRoles,
    DatabaseUser,
    SchemaNames,
    apply_bootstrap,
)
from loom.core.repository.sqlalchemy.rls.bootstrap import (
    CONFIGURE,
    SET_PASSWORD,
    scram_sha256_verifier,
)
from loom.core.repository.sqlalchemy.rls.guard_manifest import (
    GUARD_REVISIONS,
    pending_revisions,
)

SALT = b"0123456789abcdef"


def _config(**overrides: object) -> BootstrapConfig:
    config = BootstrapConfig(
        schema="s1",
        roles=DatabaseRoles(owner="r_owner", migrator="r_migrator"),
        database_users={
            "u_read": DatabaseUser(login=True, access="read"),
            "u_write": DatabaseUser(login=True, access="write"),
            "u_bypass": DatabaseUser(login=True, access="bypass"),
        },
        names=SchemaNames.derived("s1"),
    )
    return dataclasses.replace(config, **overrides)


def _names(**overrides: str) -> SchemaNames:
    return dataclasses.replace(SchemaNames.derived("s1"), **overrides)


def test_a_valid_config_validates_to_itself() -> None:
    config = _config()

    assert config.validated() is config


def test_bypass_users_keep_declaration_order() -> None:
    config = _config(
        database_users={
            "ops_b": DatabaseUser(login=True, access="bypass"),
            "u_write": DatabaseUser(login=True, access="write"),
            "ops_a": DatabaseUser(login=True, access="bypass"),
        }
    )

    assert config.bypass_users == ("ops_b", "ops_a")


@pytest.mark.parametrize(
    "bad",
    [
        {"schema": "my-schema"},
        {"schema": "1st"},
        {"roles": DatabaseRoles(owner="own;er", migrator="r_migrator")},
        {"roles": DatabaseRoles(owner="r_owner", migrator="public")},
        {"database_users": {"drop table": DatabaseUser(login=True, access="read")}},
        {"names": _names(guard="Guard")},
        {"names": _names(readers="pg_readers")},
        {"names": _names(writers="s1-writers")},
        {"names": _names(version_table="select")},
        {"names": _names(data_version_table="1st")},
    ],
)
def test_bad_identifiers_are_rejected(bad: dict[str, object]) -> None:
    config = _config(**bad)

    with pytest.raises(ValueError, match="identifier"):
        config.validated()


@pytest.mark.parametrize(
    ("field", "limit"),
    [("guard", 58), ("version_table", 59), ("data_version_table", 59), ("readers", 63)],
)
def test_names_longer_than_their_limit_are_rejected(field: str, limit: int) -> None:
    config = _config(names=_names(**{field: "n" * (limit + 1)}))

    with pytest.raises(ValueError, match=f"at most {limit}"):
        config.validated()


@pytest.mark.parametrize(
    ("field", "limit"),
    [("guard", 58), ("version_table", 59), ("data_version_table", 59), ("readers", 63)],
)
def test_names_at_their_limit_are_accepted(field: str, limit: int) -> None:
    config = _config(names=_names(**{field: "n" * limit}))

    assert config.validated() is config


def test_a_schema_longer_than_47_characters_is_rejected() -> None:
    config = _config(schema="s" * 48)

    with pytest.raises(ValueError, match="at most 47"):
        config.validated()


@pytest.mark.parametrize(
    "bad",
    [
        {"roles": DatabaseRoles(owner="r_same", migrator="r_same")},
        {"names": _names(readers="s1_group", writers="s1_group")},
        {"names": _names(readers="r_owner")},
        {"names": _names(writers="r_migrator")},
        {"database_users": {"s1_readers": DatabaseUser(login=True, access="read")}},
        {"database_users": {"r_owner": DatabaseUser(login=True, access="write")}},
    ],
)
def test_roles_must_have_distinct_names(bad: dict[str, object]) -> None:
    config = _config(**bad)

    with pytest.raises(ValueError, match="distinct names"):
        config.validated()


def test_the_two_version_tables_must_differ() -> None:
    config = _config(names=_names(version_table="versions", data_version_table="versions"))

    with pytest.raises(ValueError, match="version tables must differ"):
        config.validated()


def test_the_document_carries_every_declared_name() -> None:
    config = _config(
        names=SchemaNames(
            guard="g1",
            readers="grp_r",
            writers="grp_w",
            version_table="v_struct",
            data_version_table="v_data",
        ),
        revoke_public=False,
    )

    assert config.document() == {
        "app_schema": "s1",
        "owner_role": "r_owner",
        "migrator_role": "r_migrator",
        "readers_role": "grp_r",
        "writers_role": "grp_w",
        "users": [
            {"name": "u_read", "login": True, "access": "read"},
            {"name": "u_write", "login": True, "access": "write"},
            {"name": "u_bypass", "login": True, "access": "bypass"},
        ],
        "revoke_public": False,
        "version_table": "v_struct",
        "data_version_table": "v_data",
    }


def test_the_document_omits_the_guard_name_and_every_password() -> None:
    document = _config().document()

    assert "loom_guard_s1" not in repr(document)
    assert "password" not in repr(document).lower()


def test_the_configuration_and_the_passwords_travel_as_bound_parameters() -> None:
    assert CONFIGURE == "SELECT configure($1::jsonb)"
    assert SET_PASSWORD == "SELECT set_password_verifier($1, $2)"


def test_the_minimum_server_version_is_postgres_14() -> None:
    assert MIN_SERVER_VERSION_NUM == 140000


async def test_an_invalid_config_is_refused_before_connecting() -> None:
    config = _config(schema="my-schema")

    with pytest.raises(ConfigError, match=r"database\.schema:.*identifier"):
        await apply_bootstrap("postgresql+asyncpg://nobody@127.0.0.1:1/none", config, {})


def test_the_scram_verifier_has_the_postgres_shape_and_hides_the_password() -> None:
    verifier = scram_sha256_verifier("s3cret", salt=SALT, iterations=4096)

    head, salt_part, keys = verifier.split("$")
    iterations, salt = salt_part.split(":")
    stored_key, server_key = keys.split(":")
    assert head == "SCRAM-SHA-256"
    assert iterations == "4096"
    assert base64.b64decode(salt) == SALT
    assert len(base64.b64decode(stored_key)) == 32
    assert len(base64.b64decode(server_key)) == 32
    assert "s3cret" not in verifier


def test_the_same_password_and_salt_always_yield_the_same_verifier() -> None:
    a = scram_sha256_verifier("pw", salt=SALT, iterations=4096)
    b = scram_sha256_verifier("pw", salt=SALT, iterations=4096)

    assert a == b


def test_a_different_salt_or_password_yields_a_different_verifier() -> None:
    base = scram_sha256_verifier("pw", salt=SALT)

    assert scram_sha256_verifier("pw", salt=b"fedcba9876543210") != base
    assert scram_sha256_verifier("other", salt=SALT) != base


def test_a_fresh_guard_needs_every_revision() -> None:
    assert pending_revisions({}) == GUARD_REVISIONS


def test_a_current_guard_needs_no_revision() -> None:
    applied = {revision.number: revision.sha256 for revision in GUARD_REVISIONS}

    assert pending_revisions(applied) == ()


def test_a_revision_this_release_does_not_know_is_refused() -> None:
    applied = {revision.number: revision.sha256 for revision in GUARD_REVISIONS}
    applied[GUARD_REVISIONS[-1].number + 1] = "0" * 64

    with pytest.raises(ConfigError, match="does not know"):
        pending_revisions(applied)


def test_a_revision_recorded_with_another_digest_is_refused() -> None:
    applied = {GUARD_REVISIONS[0].number: "0" * 64}

    with pytest.raises(ConfigError, match="different digest"):
        pending_revisions(applied)
