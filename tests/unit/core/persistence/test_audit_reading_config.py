"""``stores.<name>.schema.audit.reading``: defaults, and the refusals that protect a store."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigError
from loom.core.persistence.database import decode_database_config

_POSTGRES = "postgresql+asyncpg://app@db/app"
_STORE = "stores.ledger"
_READING = r"stores\.ledger.*schema\.audit\.reading"


def _guarded(reading: dict[str, object]) -> dict[str, object]:
    return {
        "url": _POSTGRES,
        "schema": {
            "mode": "external",
            "name": "app",
            "guard": "app_guard",
            "audit": {"reading": reading},
        },
    }


def test_reading_is_absent_until_the_section_is_written() -> None:
    config = decode_database_config({"url": _POSTGRES}, path=_STORE)

    assert config.schema.audit.reading is None


def test_reading_defaults_to_disabled_and_a_thousand_subjects() -> None:
    config = decode_database_config(
        {"url": _POSTGRES, "schema": {"audit": {"reading": {}}}},
        path=_STORE,
    )

    reading = config.schema.audit.reading
    assert reading is not None
    assert reading.enabled is False
    assert reading.max_subjects == 1000
    assert reading.route is None


def test_reading_enabled_on_a_guarded_store_needs_no_timeouts() -> None:
    config = decode_database_config(_guarded({"enabled": True}), path=_STORE)

    reading = config.schema.audit.reading
    assert reading is not None
    assert reading.enabled is True
    assert reading.max_subjects == 1000
    assert reading.route is None


@pytest.mark.parametrize("prefix", ["/log", None], ids=["explicit", "omitted"])
def test_route_prefix_is_optional(prefix: str | None) -> None:
    section: dict[str, object] = {"requires_roles": ["auditor"]}
    if prefix is not None:
        section["prefix"] = prefix
    config = decode_database_config(
        _guarded({"enabled": True, "max_subjects": 25, "route": section}), path=_STORE
    )

    reading = config.schema.audit.reading
    assert reading is not None
    route = reading.route
    assert route is not None
    assert route.prefix == prefix
    assert route.requires_roles == ("auditor",)


@pytest.mark.parametrize(
    "prefix",
    [
        "",
        "audit",
        "/",
        "/audit/",
        "/two//parts",
        "/with space",
        "/tab\tpart",
        "/audit?q=1",
        "/audit#part",
    ],
)
def test_invalid_route_prefix_is_refused(prefix: str) -> None:
    with pytest.raises(ConfigError, match="prefix"):
        decode_database_config(
            _guarded({"enabled": True, "route": {"requires_roles": ["auditor"], "prefix": prefix}}),
            path=_STORE,
        )


def test_reading_disabled_does_not_require_a_guarded_store() -> None:
    config = decode_database_config(
        {
            "url": "sqlite:///:memory:",
            "schema": {"audit": {"reading": {"enabled": False, "max_subjects": 4}}},
        },
        path=_STORE,
    )

    reading = config.schema.audit.reading
    assert reading is not None
    assert reading.enabled is False
    assert reading.max_subjects == 4


def test_an_unknown_reading_key_is_refused() -> None:
    with pytest.raises(ConfigError, match="horizon"):
        decode_database_config(
            {"url": _POSTGRES, "schema": {"audit": {"reading": {"horizon": 3}}}},
            path=_STORE,
        )


def test_a_route_without_requires_roles_is_refused() -> None:
    with pytest.raises(ConfigError, match="requires_roles"):
        decode_database_config(
            {
                "url": _POSTGRES,
                "schema": {"audit": {"reading": {"route": {"prefix": "/audit"}}}},
            },
            path=_STORE,
        )


def test_an_empty_requires_roles_is_refused() -> None:
    with pytest.raises(ConfigError, match="requires_roles"):
        decode_database_config(
            {
                "url": _POSTGRES,
                "schema": {"audit": {"reading": {"route": {"requires_roles": []}}}},
            },
            path=_STORE,
        )


@pytest.mark.parametrize("role", ["", "  "])
def test_blank_requires_role_is_refused(role: str) -> None:
    with pytest.raises(ConfigError, match="requires_roles"):
        decode_database_config(
            _guarded({"enabled": True, "route": {"requires_roles": [role]}}),
            path=_STORE,
        )


def test_max_subjects_of_zero_is_refused() -> None:
    with pytest.raises(ConfigError, match="max_subjects"):
        decode_database_config(
            {"url": _POSTGRES, "schema": {"audit": {"reading": {"max_subjects": 0}}}},
            path=_STORE,
        )


def test_a_route_with_reading_disabled_is_refused() -> None:
    with pytest.raises(ConfigError, match=r"schema\.audit\.reading\.route requires enabled"):
        decode_database_config(
            {
                "url": _POSTGRES,
                "schema": {
                    "audit": {
                        "reading": {
                            "enabled": False,
                            "route": {"requires_roles": ["auditor"]},
                        }
                    }
                },
            },
            path=_STORE,
        )


@pytest.mark.parametrize(
    ("url", "schema"),
    [
        (
            _POSTGRES,
            {"mode": "create_all", "name": "app", "guard": "app_guard"},
        ),
        (
            _POSTGRES,
            {"mode": "external", "name": "app"},
        ),
        (
            _POSTGRES,
            {"mode": "external", "guard": "app_guard"},
        ),
        (
            "sqlite:///:memory:",
            {"mode": "external", "name": "app", "guard": "app_guard"},
        ),
    ],
    ids=["not-external", "no-guard", "no-name", "not-postgres"],
)
def test_reading_enabled_without_timeouts_names_the_reading_section(
    url: str, schema: dict[str, str]
) -> None:
    with pytest.raises(ConfigError, match=_READING):
        decode_database_config(
            {
                "url": url,
                "schema": {**schema, "audit": {"reading": {"enabled": True}}},
            },
            path=_STORE,
        )
