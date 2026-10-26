from __future__ import annotations

import base64
import secrets
from pathlib import Path

import msgspec
import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.repository.abc import (
    Cursor,
    CursorCodec,
    CursorConfig,
    CursorKeySource,
    PaginationMode,
    QuerySpec,
    UnsupportedQuery,
)

_QUERY = QuerySpec(pagination=PaginationMode.CURSOR)
_POSITION = Cursor(backend="sqlalchemy", keys=(), tie_breaker=1)
_ENV = "CURSOR_KEY_TEST"


def _key_text(size: int = 32) -> str:
    """A key as ``openssl rand -base64`` writes one."""
    return base64.b64encode(secrets.token_bytes(size)).decode()


def _round_trip(issuer: CursorCodec, verifier: CursorCodec) -> Cursor:
    token = issuer.encode(_POSITION, model="Invoice", query=_QUERY, scope="")
    return verifier.decode(
        token,
        backend="sqlalchemy",
        model="Invoice",
        query=_QUERY,
        scope="",
        columns={"id": int},
        primary_key=("id",),
    )


def _from_env(monkeypatch: pytest.MonkeyPatch, secret: str) -> CursorCodec:
    monkeypatch.setenv(_ENV, secret)
    return CursorCodec.from_config(
        CursorConfig(current_key_id="k1", keys={"k1": CursorKeySource(env=_ENV)})
    )


class TestDefaultMode:
    def test_without_keys_the_default_fails_and_names_the_opt_out(self) -> None:
        with pytest.raises(ConfigError, match=r"persistence\.cursors\.signing: ephemeral"):
            CursorCodec.from_config(CursorConfig())

    def test_the_default_signs_and_refuses_an_unsigned_token(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        codec = _from_env(monkeypatch, _key_text())
        unsigned = CursorCodec(signing="unsigned")

        with pytest.raises(UnsupportedQuery):
            _round_trip(unsigned, codec)


class TestSection:
    @pytest.mark.parametrize(
        "section",
        [{"signing": "sigend"}, {"singing": "ephemeral"}, {"keys": {"k1": {}}}],
        ids=["unknown-mode", "unknown-field", "sourceless-key"],
    )
    def test_an_invalid_section_fails_startup(self, section: dict[str, object]) -> None:
        ctx = ConfigContext.from_dict({"persistence": {"cursors": section}})

        with pytest.raises(ConfigError, match=r"persistence\.cursors"):
            CursorCodec.from_context(ctx)

    def test_an_absent_section_is_the_signed_default(self) -> None:
        ctx = ConfigContext.from_dict({"persistence": {"backend": "sqlalchemy"}})

        with pytest.raises(ConfigError, match=r"persistence\.cursors\.signing: ephemeral"):
            CursorCodec.from_context(ctx)


class TestKeySources:
    def test_a_key_from_the_environment_signs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        secret = _key_text()
        issuer = _from_env(monkeypatch, secret)

        assert _round_trip(issuer, _from_env(monkeypatch, secret)) == _POSITION

    def test_a_key_from_a_file_signs_like_the_same_key_from_the_environment(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        secret = _key_text()
        key_file = tmp_path / "cursor.key"
        key_file.write_text(f"{secret}\n", encoding="utf-8")
        from_file = CursorCodec.from_config(
            CursorConfig(current_key_id="k1", keys={"k1": CursorKeySource(path=str(key_file))})
        )

        assert _round_trip(from_file, _from_env(monkeypatch, secret)) == _POSITION

    def test_a_short_key_fails_without_echoing_it(self, monkeypatch: pytest.MonkeyPatch) -> None:
        secret = _key_text(31)

        with pytest.raises(ConfigError, match="32 bytes") as excinfo:
            _from_env(monkeypatch, secret)

        assert secret not in str(excinfo.value)

    def test_an_unset_variable_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(_ENV, raising=False)
        config = CursorConfig(current_key_id="k1", keys={"k1": CursorKeySource(env=_ENV)})

        with pytest.raises(ConfigError, match=_ENV):
            CursorCodec.from_config(config)

    def test_a_loom_variable_cannot_hold_a_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LOOM_CURSOR_KEY", _key_text())
        config = CursorConfig(
            current_key_id="k1", keys={"k1": CursorKeySource(env="LOOM_CURSOR_KEY")}
        )

        with pytest.raises(ConfigError, match="loom variable"):
            CursorCodec.from_config(config)

    def test_an_empty_variable_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with pytest.raises(ConfigError, match="32 bytes"):
            _from_env(monkeypatch, "  ")

    @pytest.mark.parametrize(
        "secret",
        [
            "a long passphrase is text, not random bytes in base64",
            base64.urlsafe_b64encode(b"\xfb" * 48).decode(),
            base64.b64encode(secrets.token_bytes(32)).decode().rstrip("="),
        ],
        ids=["text", "urlsafe-alphabet", "unpadded"],
    )
    def test_a_key_that_is_not_standard_base64_fails_without_echoing_it(
        self, monkeypatch: pytest.MonkeyPatch, secret: str
    ) -> None:
        with pytest.raises(ConfigError, match="base64") as excinfo:
            _from_env(monkeypatch, secret)

        assert secret not in str(excinfo.value)

    def test_an_unreadable_file_fails_without_its_path(self, tmp_path: Path) -> None:
        missing = tmp_path / "absent.key"
        config = CursorConfig(current_key_id="k1", keys={"k1": CursorKeySource(path=str(missing))})

        with pytest.raises(ConfigError, match="k1") as excinfo:
            CursorCodec.from_config(config)

        assert str(missing) not in str(excinfo.value)
        assert excinfo.value.__cause__ is None

    @pytest.mark.parametrize(
        "source", [{}, {"path": "/run/secrets/cursor", "env": _ENV}], ids=["none", "both"]
    )
    def test_a_key_needs_exactly_one_source(self, source: dict[str, str]) -> None:
        with pytest.raises(msgspec.ValidationError, match="exactly one"):
            msgspec.convert(source, CursorKeySource)

    def test_the_current_key_must_be_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(_ENV, _key_text())
        config = CursorConfig(current_key_id="k2", keys={"k1": CursorKeySource(env=_ENV)})

        with pytest.raises(ConfigError, match="current_key_id"):
            CursorCodec.from_config(config)

    def test_a_key_id_outside_the_token_alphabet_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(_ENV, _key_text())
        config = CursorConfig(current_key_id="k 1", keys={"k 1": CursorKeySource(env=_ENV)})

        with pytest.raises(ConfigError, match="key id"):
            CursorCodec.from_config(config)


class TestOptOuts:
    def test_ephemeral_needs_no_key_and_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        codec = CursorCodec.from_config(CursorConfig(signing="ephemeral"))

        assert _round_trip(codec, codec) == _POSITION
        assert any(record.levelname == "WARNING" for record in caplog.records)

    def test_unsigned_needs_no_key_and_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        codec = CursorCodec.from_config(CursorConfig(signing="unsigned"))

        assert _round_trip(codec, codec) == _POSITION
        assert any(record.levelname == "WARNING" for record in caplog.records)

    def test_an_opt_out_does_not_read_the_configured_keys(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(_ENV, raising=False)
        config = CursorConfig(signing="ephemeral", keys={"k1": CursorKeySource(env=_ENV)})

        codec = CursorCodec.from_config(config)

        assert _round_trip(codec, codec) == _POSITION
