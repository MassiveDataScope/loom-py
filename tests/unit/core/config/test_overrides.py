"""``LOOM__SECTION__KEY`` variables override the loaded configuration.

They merge last, over ``base.yaml`` and the environment overlay. A double
underscore separates the levels, each matched case-insensitively against the
keys the configuration declares; a section neither declared nor known to loom
is a startup error naming the variable. Values parse like an OmegaConf
dotlist. One line names the overridden keys, never their values.
"""

from __future__ import annotations

import logging
import traceback
from pathlib import Path
from typing import Any

import pytest
import yaml
from omegaconf import OmegaConf

from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.config.overrides import (
    apply_overrides,
    load_application_config,
    register_config_sections,
)

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
    assert not context.declares("app.resttitle")


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
        ("{read: 5, write: 10}", {"read": 5, "write": 10}),
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


@pytest.mark.parametrize(
    "variable",
    ["LOOM__CHECKS__SILENCED", "LOOM__CHECKS__MODULES", "LOOM__checks__silenced"],
)
def test_the_checks_section_cannot_be_overridden(
    config_file: str, monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    monkeypatch.setenv(variable, "[loom.rest.E001]")

    with pytest.raises(ConfigError, match="checks"):
        _load(config_file)


@pytest.mark.parametrize("value", ["${oc.env:HOME}", "${secrets:/db/url}", "pre-${x}-post"])
def test_an_interpolation_in_a_value_is_refused(
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


@pytest.mark.parametrize("section", ["LOGGER", "METRICS"])
def test_sections_loom_does_not_read_are_unknown(
    config_file: str, monkeypatch: pytest.MonkeyPatch, section: str
) -> None:
    monkeypatch.setenv(f"LOOM__{section}__LEVEL", "DEBUG")

    with pytest.raises(ConfigError, match=f"LOOM__{section}__LEVEL"):
        _load(config_file)


def test_a_registered_section_is_known(config_file: str, monkeypatch: pytest.MonkeyPatch) -> None:
    register_config_sections("overrides_test_section")
    monkeypatch.setenv("LOOM__OVERRIDES_TEST_SECTION__KEY", "x")

    assert _value(_load(config_file), "overrides_test_section.key") == "x"


def test_reaching_inside_an_interpolated_key_is_refused_without_resolving_it(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    OmegaConf.register_new_resolver("overrides_probe", lambda key: calls.append(key) or {})
    path = tmp_path / "base.yaml"
    path.write_text("database: ${overrides_probe:db}\napp: {name: x}\n", encoding="utf-8")
    config = OmegaConf.load(path)
    variables = {"LOOM__DATABASE__POOL_PRE_PING": "false"}
    try:
        with pytest.raises(ConfigError, match="LOOM__DATABASE__POOL_PRE_PING"):
            apply_overrides(config, variables)
    finally:
        OmegaConf.clear_resolver("overrides_probe")

    assert calls == []


def test_a_child_variable_wins_over_its_parent(
    config_file: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOOM__APP", "{name: parent, workers: 1}")
    monkeypatch.setenv("LOOM__APP__NAME", "child")

    context = _load(config_file)

    assert (_value(context, "app.name"), _value(context, "app.workers")) == ("child", 1)
