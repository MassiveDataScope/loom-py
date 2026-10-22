"""The connection types: inheritance by context, validation paths, secrets kept out of sight."""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.core.connections import AwsClientConfig, ClickHouseConnection, MongoConnection
from loom.core.model.types import PositiveInt


class _Context(ClickHouseConnection, frozen=True, kw_only=True):
    """A context extending the engine type: one key of its own, one default redeclared."""

    role: str
    connect_timeout: PositiveInt = 10


class _Section(msgspec.Struct, kw_only=True):
    clickhouse: ClickHouseConnection


def _decode(data: dict[str, Any], kind: type[Any]) -> Any:
    return msgspec.convert(data, kind)


def test_a_context_inherits_every_key_and_adds_its_own() -> None:
    context = _decode({"url": "clickhouse://h/db", "role": "reader", "username": "u"}, _Context)

    assert (context.url, context.username, context.role) == ("clickhouse://h/db", "u", "reader")


def test_a_context_redeclares_a_default() -> None:
    assert _decode({"url": "clickhouse://h/db", "role": "r"}, _Context).connect_timeout == 10
    assert _decode({"url": "clickhouse://h/db"}, ClickHouseConnection).connect_timeout is None


def test_frozen_and_unknown_keys_refusal_are_inherited() -> None:
    context = _decode({"url": "clickhouse://h/db", "role": "r"}, _Context)

    with pytest.raises(AttributeError):
        context.url = "other"  # type: ignore[misc]
    with pytest.raises(msgspec.ValidationError, match="unknown field `nope`"):
        _decode({"url": "clickhouse://h/db", "role": "r", "nope": 1}, _Context)


def test_an_unknown_key_names_its_section() -> None:
    with pytest.raises(msgspec.ValidationError, match=r"at `\$\.clickhouse`"):
        _decode({"clickhouse": {"url": "clickhouse://h/db", "http_url": "x"}}, _Section)


@pytest.mark.parametrize(
    ("kind", "data", "field"),
    [
        (ClickHouseConnection, {"url": "  "}, "url"),
        (MongoConnection, {"url": "", "database": "app"}, "url"),
        (MongoConnection, {"url": "mongodb://h", "database": " "}, "database"),
    ],
)
def test_an_empty_address_is_refused(kind: type[Any], data: dict[str, Any], field: str) -> None:
    with pytest.raises(msgspec.ValidationError, match=f"{field} must not be empty"):
        _decode(data, kind)


def test_aws_bounds_are_finite_by_default() -> None:
    config = AwsClientConfig()

    assert (config.connect_timeout, config.read_timeout, config.max_attempts) == (2, 5, 3)
    assert (config.retry_mode, config.pool.size) == ("standard", 32)


@pytest.mark.parametrize(
    ("connection", "secret"),
    [
        (ClickHouseConnection(url="clickhouse://user:pw1@h/db", password="pw2"), ("pw1", "pw2")),
        (_Context(url="clickhouse://user:pw1@h/db", password="pw2", role="r"), ("pw1", "pw2")),
        (MongoConnection(url="mongodb://user:pw1@h:27017", database="app"), ("pw1",)),
        (
            MongoConnection(url="mongodb://h:27017", database="app", password="pw2"),
            ("pw2",),
        ),
    ],
)
def test_the_repr_hides_every_secret(connection: object, secret: tuple[str, ...]) -> None:
    shown = repr(connection) + repr([connection])

    assert all(value not in shown for value in secret)
    assert "***" in shown


def test_a_validation_error_never_quotes_the_secret() -> None:
    with pytest.raises(msgspec.ValidationError) as caught:
        _decode(
            {"url": "clickhouse://user:pw1@h/db", "password": "pw2", "pool": {"size": 0}},
            ClickHouseConnection,
        )

    assert "pw1" not in str(caught.value)
    assert "pw2" not in str(caught.value)


@pytest.mark.parametrize(
    ("uri", "secret"),
    [
        ("clickhouse://user:p@ss@host:8123/db", "p@ss"),  # a raw "@" in the password
        ("clickhouse://user:p/ss@host:8123/db", "p/ss"),
        ("clickhouse://user:p%40ss@host:8123/db", "p%40ss"),
        ("clickhouse://host:8123/db?password=s3cr3t&compress=1", "s3cr3t"),
        ("mongodb://h/?tlsCertificateKeyFilePassword=k3y&tls=true", "k3y"),
        ("mongodb://h/?authMechanismProperties=AWS_SESSION_TOKEN:t0k3n", "t0k3n"),
    ],
)
def test_redact_uri_masks_every_credential(uri: str, secret: str) -> None:
    from loom.core.connections import redact_uri

    redacted = redact_uri(uri)

    assert secret not in redacted
    assert "***" in redacted


def test_redact_uri_keeps_what_is_not_a_credential() -> None:
    from loom.core.connections import redact_uri

    assert redact_uri("clickhouse://host:8123/db?compress=1") == (
        "clickhouse://host:8123/db?compress=1"
    )
    assert redact_uri("http://host/db?q=a@b") == "http://host/db?q=a@b"
