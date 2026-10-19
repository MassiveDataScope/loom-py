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
    assert "unclosed-secret" not in str(caught.value)


@pytest.fixture
def checks_and_auth_file(tmp_path: Path) -> str:
    """A configuration that declares checks and app.rest.auth, so only the guard can refuse."""
    document = {
        "app": {"name": "x", "rest": {"auth": {"custom": True}}},
        "checks": {"silenced": ["loom.rest.W001"], "modules": []},
    }
    path = tmp_path / "base.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return str(path)


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("LOOM__CHECKS__SILENCED", "[loom.rest.E001]"),
        ("LOOM__checks__modules", "[]"),
        ("LOOM__APP__REST__AUTH__CUSTOM", "false"),
        ("LOOM__app__Rest__Auth__JWT__AUDIENCE", "other"),
        ("LOOM__APP__REST__DOCS_URL", "/docs"),
        ("LOOM__APP__REST__REDOC_URL", "/redoc"),
        ("LOOM__app__rest__OPENAPI_URL", "/openapi.json"),
        ("LOOM__OBSERVABILITY__PROMETHEUS__CONFIG__PATH", "/metrics"),
    ],
)
def test_reviewed_keys_cannot_be_overridden(
    checks_and_auth_file: str, monkeypatch: pytest.MonkeyPatch, variable: str, value: str
) -> None:
    monkeypatch.setenv(variable, value)

    with pytest.raises(ConfigError, match="reviewed"):
        _load(checks_and_auth_file)


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("LOOM__APP", "{REST: 0, rest: {auth: {custom: false}}}"),
        ("LOOM__APP__REST", "{title: Live}"),
        ("LOOM__APP__WORKERS", "[1, {admin: true}]"),
    ],
)
def test_a_value_that_is_or_holds_a_mapping_is_refused(
    checks_and_auth_file: str, monkeypatch: pytest.MonkeyPatch, variable: str, value: str
) -> None:
    monkeypatch.setenv(variable, value)

    with pytest.raises(ConfigError, match=f"{variable}.*leaf"):
        _load(checks_and_auth_file)


@pytest.mark.parametrize(("variable", "value"), [("LOOM__APP__REST", ""), ("LOOM__APP", "null")])
def test_a_structure_the_files_declare_cannot_be_replaced(
    checks_and_auth_file: str, monkeypatch: pytest.MonkeyPatch, variable: str, value: str
) -> None:
    monkeypatch.setenv(variable, value)

    with pytest.raises(ConfigError, match=f"{variable}.*leaf"):
        _load(checks_and_auth_file)


def test_a_mapping_over_an_interpolated_section_is_refused_without_resolving_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    OmegaConf.register_new_resolver("overrides_probe2", lambda key: calls.append(key) or {})
    path = tmp_path / "base.yaml"
    path.write_text("database: ${overrides_probe2:db}\napp: {name: x}\n", encoding="utf-8")
    monkeypatch.setenv("LOOM__DATABASE", "{pool_pre_ping: false}")
    try:
        with pytest.raises(ConfigError, match="LOOM__DATABASE"):
            load_application_config(str(path))
    finally:
        OmegaConf.clear_resolver("overrides_probe2")

    assert calls == []


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
    assert secret not in str(caught.value)
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


def test_no_traceback_of_a_parse_error_carries_the_value(
    config_file: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOOM__APP__WORKERS", "[unclosed-secret")

    with pytest.raises(ConfigError) as caught:
        _load(config_file)

    assert "unclosed-secret" not in "".join(traceback.format_exception(caught.value))


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
