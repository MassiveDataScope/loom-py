"""Secret, Dsn and the redacted mapping: display hides the secret, reveal returns it."""

from __future__ import annotations

import pytest

from loom.core.model import LoomFrozenStruct
from loom.core.model.redaction import (
    Dsn,
    RedactedMap,
    Secret,
    is_secret_name,
    mask_dsn,
)


class TestRedaction:
    def test_a_secret_displays_as_a_mask_on_every_path(self) -> None:
        secret = Secret("s3cr3t")

        shown = f"{secret!r} {secret} {secret:s} {[secret]!r}"

        assert "s3cr3t" not in shown
        assert shown.count("***") == 4

    def test_reveal_returns_the_secret_and_a_copy_of_a_mapping(self) -> None:
        secret = Secret({"token": "s3cr3t"})

        revealed = secret.reveal()
        revealed["token"] = "other"

        assert secret.reveal() == {"token": "s3cr3t"}
        assert revealed is not secret.reveal()

    def test_two_secrets_are_equal_by_value(self) -> None:
        left = Secret("s3cr3t")
        right = Secret("s3cr3t")

        assert left == right
        assert left != Secret("other")

    def test_a_blank_secret_is_refused(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            Secret("  ")

    def test_an_absent_secret_is_not_a_mask(self) -> None:
        assert Secret("s3cr3t") != None  # noqa: E711
        assert str(None) != "***"

    def test_a_dsn_hides_only_the_password_and_secret_query_values(self) -> None:
        address = Dsn("postgresql+asyncpg://app:s3cr3t@db:5432/app?sslmode=require&password=s3cr3t")

        shown = f"{address!r} {address} {address:s}"

        assert "s3cr3t" not in shown
        assert "postgresql+asyncpg://app:***@db:5432/app?sslmode=require&password=***" in shown

    def test_a_dsn_without_a_secret_is_shown_whole(self) -> None:
        address = Dsn("https://example.com/path")

        assert str(address) == "https://example.com/path"
        assert address.reveal() == "https://example.com/path"

    def test_an_at_sign_in_the_path_does_not_hide_the_host(self) -> None:
        assert mask_dsn("http://host/db?q=a@b") == "http://host/db?q=a@b"
        assert mask_dsn("http://host/a@b") == "http://host/a@b"
        assert mask_dsn("http://user:s3cr3t@host/a@b") == "http://user:***@host/a@b"

    @pytest.mark.parametrize(
        ("dsn", "password"),
        [
            pytest.param(
                "clickhouse://user:p/ss@host:8123/db",
                "p/ss",
                id="a_slash_in_the_password_stays_out_of_the_display",
            ),
            pytest.param(
                "clickhouse://user:p@ss@host:8123/db",
                "p@ss",
                id="a_raw_at_sign_in_the_password_stays_out_of_the_display",
            ),
        ],
    )
    def test_a_special_character_in_the_password_stays_out_of_the_display(
        self, dsn: str, password: str
    ) -> None:
        shown = mask_dsn(dsn)

        assert password not in shown
        assert shown == "clickhouse://user:***@host:8123/db"

    def test_an_empty_password_in_an_address_is_refused(self) -> None:
        with pytest.raises(ValueError, match="password must not be empty") as caught:
            Dsn("postgresql://app:@db/app")

        assert "postgresql" not in str(caught.value)

    def test_reveal_returns_the_address_the_driver_dials(self) -> None:
        raw = "clickhouse://user:s3cr3t@host:8123/db"
        assert Dsn(raw).reveal() == raw

    def test_a_redacted_map_shows_keys_and_hides_secret_values(self) -> None:
        mapping = RedactedMap(
            {"ssl": "require", "password": "s3cr3t", "headers": {"Authorization": "Bearer s3cr3t"}}
        )

        shown = f"{mapping!r} {mapping}"

        assert "s3cr3t" not in shown
        assert "ssl" in shown
        assert "require" in shown
        assert "Authorization" in shown

    def test_reveal_of_a_redacted_map_is_a_copy(self) -> None:
        mapping = RedactedMap({"password": "s3cr3t", "ssl": "require"})

        revealed = mapping.reveal()
        revealed["password"] = "other"

        assert mapping.reveal()["password"] == "s3cr3t"

    def test_a_blank_secret_entry_is_refused_without_quoting_it(self) -> None:
        with pytest.raises(ValueError, match="password must not be empty"):
            RedactedMap({"password": "  ", "ssl": "require"})

    @pytest.mark.parametrize(
        ("name", "secret"),
        [
            ("password", True),
            ("api_key", True),
            ("signing_key", True),
            ("ssl.key.password", True),
            ("tlsCertificateKeyFilePassword", True),
            ("webhook_url", True),
            ("authorization", True),
            ("cookie", True),
            ("authMechanismProperties", True),
            ("X-Api-Key", True),
            ("keyboard", False),
            ("passive", False),
            ("primary_key", False),
            ("foreign_key", False),
            ("partition_key", False),
            ("sort_key", False),
            ("PrimaryKey", False),
        ],
    )
    def test_a_name_is_secret_by_token(self, name: str, secret: bool) -> None:
        assert is_secret_name(name) is secret

    def test_a_plain_string_is_refused_on_a_secret_field(self) -> None:
        class Settings(LoomFrozenStruct, frozen=True, kw_only=True):
            password: Secret[str] | None = None

        with pytest.raises(TypeError, match="password must be Secret"):
            Settings(password="s3cr3t")  # type: ignore[arg-type]
