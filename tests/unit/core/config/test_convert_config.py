"""The config decoder builds secrets and the dump masks them."""

from __future__ import annotations

import msgspec
import pytest

from loom.core.config.convert import convert_config, dump_config
from loom.core.model.redaction import Dsn, RedactedMap, Secret


class _Settings(msgspec.Struct, frozen=True, kw_only=True):
    url: Dsn
    password: Secret[str] | None = None
    headers: RedactedMap = msgspec.field(default_factory=lambda: RedactedMap({}))


def test_decode_builds_a_secret_and_an_address() -> None:
    settings = convert_config(
        {
            "url": "postgresql://app:s3cr3t@db/app",
            "password": "s3cr3t",
            "headers": {"Authorization": "Bearer s3cr3t", "Accept": "application/json"},
        },
        _Settings,
        strict=False,
    )

    assert settings.password is not None
    assert settings.password.reveal() == "s3cr3t"
    assert settings.url.reveal() == "postgresql://app:s3cr3t@db/app"
    assert settings.headers["Accept"] == "application/json"
    assert "s3cr3t" not in repr(settings)


def test_an_absent_password_stays_absent() -> None:
    settings = convert_config({"url": "postgresql://db/app"}, _Settings, strict=True)

    assert settings.password is None
    assert "***" not in repr(settings.password)


def test_a_validation_error_names_the_path_and_not_the_value() -> None:
    with pytest.raises(msgspec.ValidationError, match=r"\$\.password") as caught:
        convert_config(
            {"url": "postgresql://db/app", "password": 1},
            _Settings,
            strict=False,
        )

    assert "1" not in str(caught.value).split("$.password", 1)[-1] or "got `int`" in str(
        caught.value
    )
    assert "s3cr3t" not in str(caught.value)


def test_dump_masks_and_a_dump_without_the_hook_fails_closed() -> None:
    settings = _Settings(
        url=Dsn("postgresql://app:s3cr3t@db/app"),
        password=Secret("s3cr3t"),
        headers=RedactedMap({"Authorization": "Bearer s3cr3t", "Accept": "application/json"}),
    )

    dumped = dump_config(settings)

    assert dumped["password"] == "***"
    assert "s3cr3t" not in dumped["url"]
    assert dumped["headers"]["Accept"] == "application/json"
    assert dumped["headers"]["Authorization"] == "***"
    with pytest.raises(TypeError):
        msgspec.to_builtins(settings)


def test_strict_does_not_coerce_an_int_into_a_secret_string() -> None:
    with pytest.raises(msgspec.ValidationError, match="str"):
        convert_config({"url": "postgresql://db/app", "password": 1}, _Settings, strict=True)
