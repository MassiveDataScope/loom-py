"""``LOOM__SECTION__KEY`` variables override the loaded configuration.

They merge last, over ``base.yaml`` and the environment overlay. A double
underscore separates the levels, each matched case-insensitively against the
keys the configuration declares; a section neither declared nor known to loom
is a startup error naming the variable. Values parse like an OmegaConf
dotlist. One line names the overridden keys, never their values.
"""

from __future__ import annotations

import logging
import re
import traceback
from pathlib import Path
from typing import Any

import pytest
import yaml
from omegaconf import OmegaConf

from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.config.overrides import load_application_config
from loom.core.environment import Environment

_LOGGER = "loom.core.config.overrides"


@pytest.fixture
def config_file(tmp_path: Path) -> str:
    document = {"app": {"name": "base", "restTitle": "Base", "workers": 2}}
    path = tmp_path / "base.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return str(path)


def _load(config_file: str) -> ConfigContext:
    return load_application_config(config_file)


def _value(context: ConfigContext, key: str) -> Any:
    return context.section(key, object)


def test_without_variables_the_files_load_unchanged(config_file: str) -> None:
    assert _value(_load(config_file), "app.name") == "base"


def test_a_variable_overrides_the_files(config_file: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOOM__APP__NAME", "from-variable")

    assert _value(_load(config_file), "app.name") == "from-variable"


def test_the_variable_wins_over_the_environment_overlay(
    tmp_path: Path, config_file: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    overlay = tmp_path / "production.yaml"
    overlay.write_text(yaml.safe_dump({"app": {"name": "overlay"}}), encoding="utf-8")
    monkeypatch.setenv("LOOM__APP__NAME", "from-variable")

    context = load_application_config(config_file, str(overlay))

    assert _value(context, "app.name") == "from-variable"


def test_a_level_matches_the_declared_key_whatever_its_case(
    config_file: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOOM__APP__RESTTITLE", "Live")

    context = _load(config_file)

    assert _value(context, "app.restTitle") == "Live"
    assert context.raw("app.resttitle") is None


def test_an_undeclared_key_of_a_known_section_is_added_in_lower_case(
    config_file: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOOM__OBSERVABILITY__LOG__CONFIG__LEVEL", "DEBUG")

    assert _value(_load(config_file), "observability.log.config.level") == "DEBUG"


@pytest.mark.parametrize(
    ("raw", "parsed"),
    [
        ("5", 5),
        ("false", False),
        ("[https://a.example, https://b.example]", ["https://a.example", "https://b.example"]),
        ("[1, 2]", [1, 2]),
    ],
)
def test_values_parse_like_an_omegaconf_dotlist(
    config_file: str, monkeypatch: pytest.MonkeyPatch, raw: str, parsed: Any
) -> None:
    monkeypatch.setenv("LOOM__APP__WORKERS", raw)

    assert _value(_load(config_file), "app.workers") == parsed


def test_a_section_neither_declared_nor_known_is_an_error_naming_the_variable(
    config_file: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOOM__APPP__NAME", "typo")

    with pytest.raises(ConfigError, match="LOOM__APPP__NAME"):
        _load(config_file)


def test_an_empty_level_is_an_error_naming_the_variable(
    config_file: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOOM__APP____NAME", "x")

    with pytest.raises(ConfigError, match="LOOM__APP____NAME"):
        _load(config_file)


def test_one_line_names_the_overridden_keys_and_never_their_values(
    config_file: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("LOOM__APP__NAME", "value-one")
    monkeypatch.setenv("LOOM__APP__WORKERS", "77")

    with caplog.at_level(logging.INFO, logger=_LOGGER):
        _load(config_file)

    (record,) = [r for r in caplog.records if r.name == _LOGGER]
    message = record.getMessage()
    assert "app.name" in message
    assert "app.workers" in message
    assert "value-one" not in message
    assert "77" not in message


def test_no_line_without_overrides(config_file: str, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger=_LOGGER):
        _load(config_file)

    assert [r for r in caplog.records if r.name == _LOGGER] == []


def test_a_value_that_does_not_parse_is_an_error_naming_the_variable_not_the_value(
    config_file: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOOM__APP__WORKERS", "[unclosed-secret")

    with pytest.raises(ConfigError) as caught:
        _load(config_file)

    assert "LOOM__APP__WORKERS" in str(caught.value)
    assert "unclosed-secret" not in "".join(traceback.format_exception(caught.value))


_DEPLOYED_DOCUMENT: dict[str, Any] = {
    "shared": {"audience": "api"},
    "app": {
        "name": "x",
        "code_path": "src",
        "rest": {
            "title": "Base",
            "auth": {"jwt": {"audience": "${shared.audience}"}},
            "interfaces": {"orders": {"prefix": "/orders"}},
            "disable_routes": [{"method": "GET", "path": "/orders/"}],
        },
        "matrix": [[{"cell": 1}]],
    },
    "observability": {"log": {"config": {"level": "INFO"}}, "prometheus": {"enabled": True}},
    "checks": {"silenced": ["loom.rest.W001"]},
    "database": {
        "url": "postgresql+asyncpg://app@db/app",
        "pool": {"size": 5},
        "warehouse": {"url": "postgresql+asyncpg://app@wh/wh", "pool": {"size": 2}},
    },
    "persistence": {"dynamodb": {"region": "eu-west-1", "table": "t", "read_timeout": 5}},
}


def _write_document(tmp_path: Path, document: dict[str, Any]) -> str:
    path = tmp_path / "base.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return str(path)


def _load_deployed(path: str) -> ConfigContext:
    return load_application_config(path, environment=Environment.PRODUCTION)


@pytest.fixture
def deployed_file(tmp_path: Path) -> str:
    return _write_document(tmp_path, _DEPLOYED_DOCUMENT)


@pytest.mark.parametrize(
    ("variable", "value", "key"),
    [
        ("LOOM__OBSERVABILITY__LOG__CONFIG__LEVEL", "DEBUG", "observability.log.config.level"),
        (
            "LOOM__OBSERVABILITY__LOG__CONFIG__NAMED_LEVELS__ASYNCIO",
            "WARNING",
            "observability.log.config.named_levels.asyncio",
        ),
        ("LOOM__CELERY__WORKER_CONCURRENCY", "8", "celery.worker_concurrency"),
        (
            "LOOM__SQL__CONNECTIONS__ANALYTICS__CONNECT_TIMEOUT",
            "5",
            "sql.connections.analytics.connect_timeout",
        ),
        ("LOOM__DATABASE__POOL__SIZE", "1", "database.pool.size"),
        ("LOOM__DATABASE__POOL__MAX_OVERFLOW", "0", "database.pool.max_overflow"),
        ("LOOM__DATABASE__POOL__TIMEOUT", "5", "database.pool.timeout"),
        ("LOOM__DATABASE__POOL__RECYCLE", "300", "database.pool.recycle"),
        ("LOOM__DATABASE__CONNECT_TIMEOUT", "5", "database.connect_timeout"),
        ("LOOM__DATABASE__READ_TIMEOUT", "30", "database.read_timeout"),
        ("LOOM__PERSISTENCE__DYNAMODB__READ_TIMEOUT", "2", "persistence.dynamodb.read_timeout"),
        (
            "LOOM__PERSISTENCE__DYNAMODB__CONNECT_TIMEOUT",
            "1",
            "persistence.dynamodb.connect_timeout",
        ),
        ("LOOM__PERSISTENCE__DYNAMODB__MAX_ATTEMPTS", "2", "persistence.dynamodb.max_attempts"),
        (
            "LOOM__PERSISTENCE__DYNAMODB__POOL__SIZE",
            "8",
            "persistence.dynamodb.pool.size",
        ),
        ("LOOM__STORAGE__DYNAMODB__READ_TIMEOUT", "9", "storage.dynamodb.read_timeout"),
        ("LOOM__STORAGE__DYNAMODB__CONNECT_TIMEOUT", "1", "storage.dynamodb.connect_timeout"),
        ("LOOM__STORAGE__DYNAMODB__MAX_ATTEMPTS", "2", "storage.dynamodb.max_attempts"),
        (
            "LOOM__STORAGE__DYNAMODB__POOL__SIZE",
            "8",
            "storage.dynamodb.pool.size",
        ),
    ],
)
def test_a_deployed_environment_takes_overrides_of_loom_operational_keys(
    deployed_file: str, monkeypatch: pytest.MonkeyPatch, variable: str, value: str, key: str
) -> None:
    monkeypatch.setenv(variable, value)

    assert str(_value(_load_deployed(deployed_file), key)) == value


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("LOOM__SHARED__AUDIENCE", "other"),
        ("LOOM__OBSERVABILITY__PROMETHEUS__ENABLED", "false"),
        ("LOOM__APP__REST__INTERFACES__ORDERS__PREFIX", "/admin"),
        ("LOOM__APP__CODE_PATH", "/tmp/elsewhere"),
        ("LOOM__APP__REST__AUTH__JWT__AUDIENCE", "other"),
        ("LOOM__CHECKS__SILENCED", "[loom.rest.E001]"),
        ("LOOM__APP__REST__DOCS_URL", "/docs"),
        ("LOOM__OBSERVABILITY__LOG__CONFIG__LEVELS", "DEBUG"),
        ("LOOM__SQL__CONNECTIONS__ANALYTICS__MAX_EXECUTION_TIME", "0"),
        ("LOOM__APP__REST__INTERFACES__ORDERS__FILTERABLE", "all"),
        ("LOOM__APP__REST__INTERFACES__ORDERS__SORTABLE", "[total]"),
        ("LOOM__PERSISTENCE__CURSORS__SIGNING", "unsigned"),
        ("LOOM__DATABASE__URL", "postgresql+asyncpg://evil@db/app"),
        ("LOOM__DATABASE__POOL__PRE_PING", "false"),
        ("LOOM__DATABASE__WAREHOUSE__POOL__SIZE", "1"),
    ],
    ids=[
        "a key a reviewed key references, off the allowlist",
        "prometheus enabled",
        "interfaces",
        "code_path",
        "auth",
        "checks",
        "a documentation path",
        "a key one character off an allowlisted one",
        "the query time limit",
        "the columns a list route filters on",
        "the columns a list route sorts by",
        "cursor signing",
        "the database url",
        "the pre-ping safety check",
        "a named database, which a wildcard would confuse with the section's own keys",
    ],
)
def test_a_deployed_environment_refuses_every_key_off_the_allowlist(
    deployed_file: str, monkeypatch: pytest.MonkeyPatch, variable: str, value: str
) -> None:
    monkeypatch.setenv(variable, value)

    with pytest.raises(ConfigError, match=f"{variable}.*not overridable in 'production'"):
        _load_deployed(deployed_file)


def test_the_files_extend_the_allowlist_under_config_overridable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    document = {**_DEPLOYED_DOCUMENT, "config": {"overridable": ["app.rest.title"]}}
    monkeypatch.setenv("LOOM__APP__REST__TITLE", "Live")

    context = _load_deployed(_write_document(tmp_path, document))

    assert _value(context, "app.rest.title") == "Live"


def test_an_allowlist_entry_covers_one_key_and_not_its_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    document = {**_DEPLOYED_DOCUMENT, "config": {"overridable": ["app.rest.title"]}}
    monkeypatch.setenv("LOOM__APP__REST__TITLES", "Live")

    with pytest.raises(ConfigError, match="LOOM__APP__REST__TITLES"):
        _load_deployed(_write_document(tmp_path, document))


@pytest.mark.parametrize(
    "overridable",
    [["${oc.env:KEYS}"], "app.rest.title", ["app.rest.ti-tle"], ["app..title"]],
)
def test_config_overridable_must_be_a_literal_list_of_dotted_keys_in_any_environment(
    tmp_path: Path, overridable: object
) -> None:
    document = {**_DEPLOYED_DOCUMENT, "config": {"overridable": overridable}}
    path = _write_document(tmp_path, document)

    with pytest.raises(ConfigError, match="config.overridable must be a literal list"):
        load_application_config(path, environment=Environment.DEVELOPMENT)


@pytest.mark.parametrize(
    "entry",
    [
        "*.level",
        "checks",
        "checks.silenced",
        "CHECKS.modules",
        "config.overridable",
        "config.*",
        "persistence.cursors.signing",
        "Persistence.Cursors.current_key_id",
        "persistence.*.signing",
        "persistence.cursors.keys.*.env",
    ],
)
def test_config_overridable_refuses_an_entry_that_opens_a_protected_key(
    tmp_path: Path, entry: str
) -> None:
    document = {**_DEPLOYED_DOCUMENT, "config": {"overridable": ["app.rest.title", entry]}}
    path = _write_document(tmp_path, document)

    with pytest.raises(ConfigError, match=f"config.overridable entry {re.escape(repr(entry))}"):
        load_application_config(path, environment=Environment.DEVELOPMENT)


@pytest.mark.parametrize(
    "entry", ["app.code_path", "app.*", "app.rest.auth.custom", "persistence.backend"]
)
def test_config_overridable_accepts_other_keys_since_a_deploy_check_judges_the_merged_value(
    tmp_path: Path, entry: str
) -> None:
    document = {**_DEPLOYED_DOCUMENT, "config": {"overridable": [entry]}}

    context = load_application_config(
        _write_document(tmp_path, document), environment=Environment.PRODUCTION
    )

    assert context.raw("config.overridable") == [entry]


@pytest.mark.parametrize("environment", list(Environment))
def test_config_overridable_itself_can_never_be_overridden(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, environment: Environment
) -> None:
    document = {**_DEPLOYED_DOCUMENT, "config": {"overridable": ["app.rest.title"]}}
    path = _write_document(tmp_path, document)
    monkeypatch.setenv("LOOM__CONFIG__OVERRIDABLE", "[app.code_path]")

    with pytest.raises(
        ConfigError,
        match="LOOM__CONFIG__OVERRIDABLE overrides 'config.overridable'; config.overridable "
        "is declared only in the configuration files",
    ):
        load_application_config(path, environment=environment)


def test_a_local_environment_takes_any_leaf(
    deployed_file: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOOM__APP__CODE_PATH", "elsewhere")

    context = load_application_config(deployed_file, environment=Environment.DEVELOPMENT)

    assert _value(context, "app.code_path") == "elsewhere"


@pytest.mark.parametrize(
    ("variable", "value"),
    [("LOOM__APP__EXTRA", "{admin: true}"), ("LOOM__APP__EXTRA", "[1, {admin: true}]")],
)
def test_a_value_that_is_or_holds_a_mapping_is_refused(
    deployed_file: str, monkeypatch: pytest.MonkeyPatch, variable: str, value: str
) -> None:
    monkeypatch.setenv(variable, value)

    with pytest.raises(ConfigError, match=f"{variable} is or holds a mapping"):
        load_application_config(deployed_file, environment=Environment.DEVELOPMENT)


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("LOOM__APP__REST", ""),
        ("LOOM__APP", "null"),
        ("LOOM__APP__REST__DISABLE_ROUTES", "[]"),
        ("LOOM__APP__MATRIX", "[]"),
    ],
)
def test_a_structure_the_files_declare_cannot_be_replaced(
    deployed_file: str, monkeypatch: pytest.MonkeyPatch, variable: str, value: str
) -> None:
    monkeypatch.setenv(variable, value)

    with pytest.raises(ConfigError, match=f"{variable} targets"):
        load_application_config(deployed_file, environment=Environment.DEVELOPMENT)


@pytest.mark.parametrize(
    "value", ["!!int s3cr3t-value", "!!float s3cr3t-value", "!!timestamp 2024-02-30"]
)
def test_a_tagged_value_that_fails_to_construct_never_echoes_the_value(
    config_file: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("LOOM__APP__NAME", value)

    with pytest.raises(ConfigError) as caught:
        _load(config_file)

    secret = value.split(" ", 1)[1]
    assert secret not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize(
    "value",
    [
        "${oc.env:HOME}",
        "pre-${x}-post",
        '"${oc.env:DATABASE_URL}"',
        '"\\x24{oc.env:DATABASE_URL}"',
        "[a, '${oc.env:HOME}']",
    ],
)
def test_an_interpolation_in_a_value_is_refused_however_it_is_written(
    config_file: str, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("LOOM__APP__NAME", value)

    with pytest.raises(ConfigError, match="LOOM__APP__NAME"):
        _load(config_file)


@pytest.mark.parametrize("section", ["LOGGER", "METRICS", "PARAMS"])
def test_sections_no_application_loader_reads_are_unknown(
    config_file: str, monkeypatch: pytest.MonkeyPatch, section: str
) -> None:
    monkeypatch.setenv(f"LOOM__{section}__LEVEL", "DEBUG")

    with pytest.raises(ConfigError, match=f"LOOM__{section}__LEVEL"):
        _load(config_file)


@pytest.mark.parametrize(
    "variable",
    ["LOOM__APP__NA.ME", "LOOM__APP__NAME[0]", "LOOM__APP__NA-ME", "LOOM__APP__NAMÉ"],
)
def test_a_level_outside_letters_digits_and_underscore_is_refused(
    config_file: str, monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    monkeypatch.setenv(variable, "x")

    with pytest.raises(ConfigError, match=re.escape(variable)):
        _load(config_file)


@pytest.mark.parametrize(
    "variables",
    [
        {"LOOM__APP__NAME": "a", "LOOM__app__name": "b"},
        {"LOOM__APP__EXTRA": "a", "LOOM__APP__EXTRA__INNER": "b"},
    ],
)
def test_two_variables_for_one_key_or_a_key_and_its_parent_are_refused(
    config_file: str, monkeypatch: pytest.MonkeyPatch, variables: dict[str, str]
) -> None:
    for name, value in variables.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(ConfigError) as caught:
        _load(config_file)

    assert all(name in str(caught.value) for name in variables)


def test_reaching_inside_an_interpolated_key_is_refused_without_resolving_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    OmegaConf.register_new_resolver("overrides_probe", lambda key: calls.append(key) or {})
    path = tmp_path / "base.yaml"
    path.write_text("database: ${overrides_probe:db}\napp: {name: x}\n", encoding="utf-8")
    monkeypatch.setenv("LOOM__DATABASE__POOL_PRE_PING", "false")
    try:
        with pytest.raises(ConfigError, match="LOOM__DATABASE__POOL_PRE_PING"):
            load_application_config(str(path))
    finally:
        OmegaConf.clear_resolver("overrides_probe")

    assert calls == []
