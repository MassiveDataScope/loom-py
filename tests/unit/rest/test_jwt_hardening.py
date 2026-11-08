"""JWT verification hardening (spec 010, ISO/IEC 27001:2022 A.8.5 and A.8.24).

An HMAC key shorter than its hash is refused everywhere (RFC 7518 §3.2 MUST);
the clock-skew leeway is bounded; an explicit token type can be required; and a
deployed API must name the audience and issuer its tokens are minted for.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import jwt as pyjwt
import pytest

from loom.core.checks import run_deploy_checks
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.rest.auth import JwtAuthConfig, JwtAuthenticator, RequestCredentials
from loom.rest.auth.config import JwtIssuerConfig, SharedSecretFile
from loom.rest.checks import AUDIENCE_CHECK, ISSUER_CHECK
from loom.testing import jwt_test_auth

_NO_DOCS: dict[str, Any] = {"docs_url": None, "redoc_url": None, "openapi_url": None}


def _secret_file(tmp_path: Path, size: int) -> str:
    path = tmp_path / "hs.key"
    path.write_bytes(b"k" * size)
    return str(path)


def _credentials(token: str) -> RequestCredentials:
    return RequestCredentials(headers={"authorization": f"Bearer {token}"}, path="/x")


class TestHmacKeyStrength:
    @pytest.mark.parametrize(
        ("algorithm", "minimum"), [("HS256", 32), ("HS384", 48), ("HS512", 64)]
    )
    def test_a_verifier_key_shorter_than_its_hash_is_refused(
        self, tmp_path: Path, algorithm: str, minimum: int
    ) -> None:
        config = JwtAuthConfig(
            secret_path=_secret_file(tmp_path, minimum - 1), algorithms=(algorithm,)
        )

        with pytest.raises(ConfigError, match=f"at least {minimum} bytes"):
            JwtAuthenticator(config)

    def test_a_verifier_key_of_the_hash_size_is_accepted(self, tmp_path: Path) -> None:
        config = JwtAuthConfig(secret_path=_secret_file(tmp_path, 32), algorithms=("HS256",))

        assert JwtAuthenticator(config).name == "jwt"

    def test_a_secret_signs_with_exactly_one_algorithm(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="exactly one algorithm"):
            JwtAuthConfig(secret_path=_secret_file(tmp_path, 64), algorithms=("HS256", "HS512"))

    def test_a_trailing_line_break_does_not_count_toward_the_size(self, tmp_path: Path) -> None:
        path = tmp_path / "hs.key"
        path.write_bytes(b"k" * 31 + b"\n")
        config = JwtAuthConfig(secret_path=str(path), algorithms=("HS256",))

        with pytest.raises(ConfigError, match="at least 32 bytes"):
            JwtAuthenticator(config)

    @pytest.mark.parametrize(("algorithm", "minimum"), [("HS384", 48), ("HS512", 64)])
    def test_the_issuer_minimum_follows_its_algorithm(
        self, tmp_path: Path, algorithm: str, minimum: int
    ) -> None:
        config = JwtIssuerConfig(
            signing_key=SharedSecretFile(_secret_file(tmp_path, minimum - 1)),
            algorithm=algorithm,
            audience="api",
            issuer="gateway",
            roles_claim="roles",
        )

        with pytest.raises(ConfigError, match=f"at least {minimum} bytes"):
            config.load_signing_key()

    def test_the_refusal_never_echoes_the_key(self, tmp_path: Path) -> None:
        path = tmp_path / "hs.key"
        path.write_text("short-secret-value")
        config = JwtAuthConfig(secret_path=str(path), algorithms=("HS256",))

        with pytest.raises(ConfigError) as refused:
            JwtAuthenticator(config)

        assert "short-secret-value" not in str(refused.value)

    def test_an_issuer_key_shorter_than_its_hash_is_refused(self, tmp_path: Path) -> None:
        config = JwtIssuerConfig(
            signing_key=SharedSecretFile(_secret_file(tmp_path, 31)),
            algorithm="HS256",
            audience="api",
            issuer="gateway",
            roles_claim="roles",
        )

        with pytest.raises(ConfigError, match="at least 32 bytes"):
            config.load_signing_key()


class TestLeeway:
    def test_a_leeway_above_five_minutes_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="leeway_seconds"):
            JwtAuthConfig(
                secret_path=_secret_file(tmp_path, 32), algorithms=("HS256",), leeway_seconds=301
            )

    def test_a_leeway_of_five_minutes_is_accepted(self, tmp_path: Path) -> None:
        config = JwtAuthConfig(
            secret_path=_secret_file(tmp_path, 32), algorithms=("HS256",), leeway_seconds=300
        )

        assert config.leeway_seconds == 300


class TestTokenType:
    def _token(self, secret: bytes, typ: str | None) -> str:
        headers: dict[str, Any] = {"typ": typ}
        payload = {"sub": "ada", "exp": int(time.time()) + 60}
        return pyjwt.encode(payload, secret, algorithm="HS256", headers=headers)

    def _authenticator(self, tmp_path: Path, **overrides: Any) -> JwtAuthenticator:
        config = JwtAuthConfig(
            secret_path=_secret_file(tmp_path, 32), algorithms=("HS256",), **overrides
        )
        return JwtAuthenticator(config)

    @pytest.mark.parametrize("typ", ["at+jwt", "AT+JWT", "application/at+jwt"])
    async def test_a_listed_type_is_accepted(self, tmp_path: Path, typ: str) -> None:
        authenticator = self._authenticator(tmp_path, accepted_types=("at+jwt",))

        identity = await authenticator.authenticate(_credentials(self._token(b"k" * 32, typ)))

        assert identity is not None

    @pytest.mark.parametrize("typ", ["JWT", None])
    async def test_another_or_missing_type_is_refused(
        self, tmp_path: Path, typ: str | None
    ) -> None:
        authenticator = self._authenticator(tmp_path, accepted_types=("at+jwt",))
        token = self._token(b"k" * 32, typ)

        assert pyjwt.get_unverified_header(token).get("typ") == typ
        assert await authenticator.authenticate(_credentials(token)) is None

    async def test_no_listed_type_accepts_any(self, tmp_path: Path) -> None:
        authenticator = self._authenticator(tmp_path)

        assert await authenticator.authenticate(_credentials(self._token(b"k" * 32, None)))

    def test_a_blank_listed_type_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="accepted_types"):
            JwtAuthConfig(
                secret_path=_secret_file(tmp_path, 32), algorithms=("HS256",), accepted_types=(" ",)
            )


def _rest_ids(rest: dict[str, Any], **app: Any) -> list[str]:
    report = run_deploy_checks(ConfigContext.from_dict({"app": {"name": "d", "rest": rest}, **app}))
    return [r.id for r in report.results if r.id.startswith("loom.rest.")]


class TestAudienceAndIssuerChecks:
    def test_a_jwt_without_audience_or_issuer_fails_both_checks(self) -> None:
        rest = {**_NO_DOCS, "auth": {"jwt": {"secret_path": "/k"}}}

        assert _rest_ids(rest) == [AUDIENCE_CHECK, ISSUER_CHECK]

    def test_a_jwt_naming_both_passes(self) -> None:
        jwt = {"secret_path": "/k", "audience": "api", "issuer": "${oc.env:ISSUER}"}

        assert _rest_ids({**_NO_DOCS, "auth": {"jwt": jwt}}) == []

    def test_a_custom_authenticator_is_not_judged(self) -> None:
        assert _rest_ids({**_NO_DOCS, "auth": {"custom": True}}) == []

    def test_a_lambda_function_jwt_is_judged_too(self) -> None:
        jwt = {"secret_path": "/k", "audience": "api", "issuer": "gw"}
        function = {
            "trigger": {
                "source": "http",
                "interfaces": ["a"],
                "auth": {"jwt": {"secret_path": "/k"}},
            }
        }

        ids = _rest_ids(
            {**_NO_DOCS, "auth": {"jwt": jwt}}, **{"lambda": {"functions": {"f": function}}}
        )

        assert ids == [AUDIENCE_CHECK, ISSUER_CHECK]


class TestTestingHelper:
    async def test_the_helper_mints_tokens_its_config_accepts(self, tmp_path: Path) -> None:
        auth = jwt_test_auth(tmp_path)

        identity = await JwtAuthenticator(auth.config).authenticate(_credentials(auth.token("ada")))

        assert identity is not None and identity.subject == "ada"
        assert auth.config.audience and auth.config.issuer


class TestResolvedClaims:
    def test_a_blank_audience_or_issuer_is_refused(self, tmp_path: Path) -> None:
        for field in ("audience", "issuer"):
            settings: dict[str, Any] = {field: "  "}
            with pytest.raises(ConfigError, match="must not be blank"):
                JwtAuthConfig(
                    secret_path=_secret_file(tmp_path, 32), algorithms=("HS256",), **settings
                )

    @pytest.mark.parametrize("environment", ["production", "staging"])
    def test_a_claim_resolved_to_nothing_refuses_a_deployed_start(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, environment: str
    ) -> None:
        from loom.rest.fastapi._auto_auth import _require_deployed_jwt_claims  # noqa: PLC0415

        monkeypatch.setenv("LOOM_ENV", environment)
        config = JwtAuthConfig(
            secret_path=_secret_file(tmp_path, 32), algorithms=("HS256",), issuer="idp"
        )

        with pytest.raises(ConfigError, match=AUDIENCE_CHECK):
            _require_deployed_jwt_claims(ConfigContext.from_dict({}), config)

    def test_development_and_silencing_let_it_start(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from loom.rest.fastapi._auto_auth import _require_deployed_jwt_claims  # noqa: PLC0415

        config = JwtAuthConfig(secret_path=_secret_file(tmp_path, 32), algorithms=("HS256",))
        monkeypatch.setenv("LOOM_ENV", "development")
        _require_deployed_jwt_claims(ConfigContext.from_dict({}), config)

        monkeypatch.setenv("LOOM_ENV", "production")
        silenced = ConfigContext.from_dict({"checks": {"silenced": [AUDIENCE_CHECK, ISSUER_CHECK]}})
        _require_deployed_jwt_claims(silenced, config)


class TestIssuedType:
    async def test_the_issuer_stamps_the_type_the_verifier_requires(self, tmp_path: Path) -> None:
        from loom.core.identity import Identity  # noqa: PLC0415
        from loom.rest.auth import JwtIssuer  # noqa: PLC0415

        key = _secret_file(tmp_path, 32)
        issuer = JwtIssuer(
            JwtIssuerConfig(
                signing_key=SharedSecretFile(key),
                algorithm="HS256",
                audience="api",
                issuer="gw",
                roles_claim="roles",
                typ="at+jwt",
            )
        )
        verifier = JwtAuthenticator(
            JwtAuthConfig(
                secret_path=key,
                algorithms=("HS256",),
                audience="api",
                issuer="gw",
                accepted_types=("at+jwt",),
            )
        )
        token = issuer.issue(Identity(subject="ada", roles=("r",), mechanism="jwt")).token

        assert pyjwt.get_unverified_header(token)["typ"] == "at+jwt"
        identity = await verifier.authenticate(_credentials(token))
        assert identity is not None
        assert identity.subject == "ada"


class TestTestingHelperOwnsItsKey:
    def test_it_refuses_to_override_its_own_key(self, tmp_path: Path) -> None:
        with pytest.raises(TypeError, match="algorithms"):
            jwt_test_auth(tmp_path, algorithms=("HS512",))


class TestKeyMaterialStaysOut:
    """No configuration object may carry or print key material."""

    def test_the_loaded_secret_never_appears_in_a_repr(self, tmp_path: Path) -> None:
        path = tmp_path / "hs.key"
        path.write_text("S" * 31 + "Z")
        config = JwtAuthConfig(secret_path=str(path), algorithms=("HS256",))

        assert "S" * 31 not in repr(config._keyring())

    def test_a_private_key_is_refused_in_public_keys(self) -> None:
        private = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEI\n-----END PRIVATE KEY-----"
        with pytest.raises(ConfigError, match="private key"):
            JwtAuthConfig(public_keys={"k1": private}, algorithms=("EdDSA",))
