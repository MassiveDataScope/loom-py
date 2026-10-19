from __future__ import annotations

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
_ENV = "LOOM_TEST_CURSOR_KEY"


def _round_trip(issuer: CursorCodec, verifier: CursorCodec) -> Cursor:
    token = issuer.encode(_POSITION, model="Invoice", query=_QUERY, scope="")
    return verifier.decode(token, backend="sqlalchemy", model="Invoice", query=_QUERY, scope="")


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
        codec = _from_env(monkeypatch, secrets.token_urlsafe(32))
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
        secret = secrets.token_urlsafe(32)
        issuer = _from_env(monkeypatch, secret)

        assert _round_trip(issuer, _from_env(monkeypatch, secret)) == _POSITION

    def test_a_key_from_a_file_signs_like_the_same_key_from_the_environment(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        secret = secrets.token_urlsafe(32)
        key_file = tmp_path / "cursor.key"
        key_file.write_text(f"{secret}\n", encoding="utf-8")
        from_file = CursorCodec.from_config(
            CursorConfig(current_key_id="k1", keys={"k1": CursorKeySource(path=str(key_file))})
        )

        assert _round_trip(from_file, _from_env(monkeypatch, secret)) == _POSITION

    def test_a_short_key_fails_without_echoing_it(self, monkeypatch: pytest.MonkeyPatch) -> None:
        secret = secrets.token_hex(15)

        with pytest.raises(ConfigError, match="32 bytes") as excinfo:
            _from_env(monkeypatch, secret)

        assert secret not in str(excinfo.value)

    def test_an_unset_variable_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(_ENV, raising=False)
        config = CursorConfig(current_key_id="k1", keys={"k1": CursorKeySource(env=_ENV)})

        with pytest.raises(ConfigError, match=_ENV):
            CursorCodec.from_config(config)

    def test_an_empty_variable_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with pytest.raises(ConfigError, match="empty"):
            _from_env(monkeypatch, "  ")

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
        monkeypatch.setenv(_ENV, secrets.token_urlsafe(32))
        config = CursorConfig(current_key_id="k2", keys={"k1": CursorKeySource(env=_ENV)})

        with pytest.raises(ConfigError, match="current_key_id"):
            CursorCodec.from_config(config)

    def test_a_key_id_outside_the_token_alphabet_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(_ENV, secrets.token_urlsafe(32))
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
