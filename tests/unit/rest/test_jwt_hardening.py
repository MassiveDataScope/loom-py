"""JWT verification hardening (spec 010, ISO/IEC 27001:2022 A.8.5 and A.8.24).

An HMAC key shorter than its hash is refused everywhere (RFC 7518 §3.2 MUST);
the clock-skew leeway is bounded; an explicit token type can be required; and a
deployed API must name the audience and issuer its tokens are minted for.
"""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any

import jwt as pyjwt
import pytest

from loom.core.checks import run_deploy_checks
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.environment import Environment
from loom.core.model.redaction import Secret
from loom.rest.auth import JwtAuthConfig, JwtAuthenticator, RequestCredentials
from loom.rest.auth.config import JwtIssuerConfig, JwtPublicKey
from loom.rest.checks import AUDIENCE_CHECK, ISSUER_CHECK
from loom.rest.fastapi._auto_auth import _require_deployed_jwt_claims
from loom.rest.fastapi.auto import create_app
from loom.testing import jwt_test_auth
from tests.unit.rest._fixture_app import write_project
from tests.unit.rest.test_deploy_checks import _signed_cursors

_NO_DOCS: dict[str, Any] = {"docs_url": None, "redoc_url": None, "openapi_url": None}


def _secret_file(tmp_path: Path, size: int) -> str:
    path = tmp_path / "hs.key"
    path.write_bytes(b"k" * size)
    return str(path)


def _issuer(signing_key: str, algorithm: str = "HS256", **overrides: Any) -> JwtIssuerConfig:
    return JwtIssuerConfig(
        signing_key=Secret(signing_key),
        algorithm=algorithm,  # type: ignore[arg-type]
        audience="api",
        issuer="gateway",
        roles_claim="roles",
        **overrides,
    )


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
    def test_the_issuer_minimum_follows_its_algorithm(self, algorithm: str, minimum: int) -> None:
        with pytest.raises(ConfigError, match=f"at least {minimum} bytes"):
            _issuer("k" * (minimum - 1), algorithm)

    def test_a_verifier_secret_is_judged_when_the_config_is_built(self) -> None:
        with pytest.raises(ConfigError, match="at least 32 bytes"):
            JwtAuthConfig(secret=Secret("k" * 31), algorithms=("HS256",))

    def test_a_pem_key_is_never_a_shared_secret(self) -> None:
        """Algorithm confusion: an HS secret that is a public key lets its holders sign."""
        pem = "-----BEGIN PUBLIC KEY-----\n" + "A" * 64 + "\n-----END PUBLIC KEY-----"
        with pytest.raises(ConfigError, match="is an asymmetric key"):
            JwtAuthConfig(secret=Secret(pem), algorithms=("HS256",))

    def test_the_refusal_never_echoes_the_key(self, tmp_path: Path) -> None:
        path = tmp_path / "hs.key"
        path.write_text("short-secret-value")
        config = JwtAuthConfig(secret_path=str(path), algorithms=("HS256",))

        with pytest.raises(ConfigError) as refused:
            JwtAuthenticator(config)

        assert "short-secret-value" not in str(refused.value)

    def test_an_issuer_key_shorter_than_its_hash_is_refused(self) -> None:
        with pytest.raises(ConfigError, match="at least 32 bytes"):
            _issuer("k" * 31)


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
    async def test_the_helper_mints_tokens_its_config_accepts(self) -> None:
        auth = jwt_test_auth()

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

    @pytest.mark.parametrize("environment", [Environment.PRODUCTION, Environment.STAGING])
    def test_a_claim_resolved_to_nothing_refuses_a_deployed_start(
        self, tmp_path: Path, environment: Environment
    ) -> None:
        config = JwtAuthConfig(
            secret_path=_secret_file(tmp_path, 32), algorithms=("HS256",), issuer="idp"
        )

        with pytest.raises(ConfigError, match=AUDIENCE_CHECK):
            _require_deployed_jwt_claims(ConfigContext.from_dict({}), environment, config)

    def test_development_and_silencing_let_it_start(self, tmp_path: Path) -> None:
        config = JwtAuthConfig(secret_path=_secret_file(tmp_path, 32), algorithms=("HS256",))
        _require_deployed_jwt_claims(ConfigContext.from_dict({}), Environment.DEVELOPMENT, config)

        silenced = ConfigContext.from_dict({"checks": {"silenced": [AUDIENCE_CHECK, ISSUER_CHECK]}})
        _require_deployed_jwt_claims(silenced, Environment.PRODUCTION, config)

    def test_the_composed_environment_wins_over_the_process_one(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``LOOM_ENV=development`` must not relax an app composed for production."""
        monkeypatch.setenv("LOOM_ENV", "development")
        monkeypatch.delenv("NOPE_AUD", raising=False)
        jwt = {
            "secret_path": _secret_file(tmp_path, 32),
            "algorithms": ["HS256"],
            "audience": "${oc.env:NOPE_AUD,null}",
            "issuer": "idp",
        }
        project = tmp_path / "app"
        project.mkdir()
        config = write_project(
            project, rest={**_NO_DOCS, "auth": {"jwt": jwt}}, persistence=_signed_cursors(project)
        )

        with pytest.raises(ConfigError, match=AUDIENCE_CHECK):
            create_app(config, environment="production")


class TestIssuedType:
    async def test_the_issuer_stamps_the_type_the_verifier_requires(self) -> None:
        from loom.core.identity import Identity  # noqa: PLC0415
        from loom.rest.auth import JwtIssuer  # noqa: PLC0415

        config = _issuer("k" * 32, typ="at+jwt")
        issuer = JwtIssuer(config)
        verifier = JwtAuthenticator(JwtAuthConfig.for_issuer(config))
        token = issuer.issue(Identity(subject="ada", roles=("r",), mechanism="jwt")).token

        assert pyjwt.get_unverified_header(token)["typ"] == "at+jwt"
        identity = await verifier.authenticate(_credentials(token))
        assert identity is not None
        assert identity.subject == "ada"


class TestTestingHelperOwnsItsKey:
    def test_it_refuses_to_override_its_own_key(self) -> None:
        with pytest.raises(TypeError, match="algorithms"):
            jwt_test_auth(algorithms=("HS512",))


class TestKeyMaterialStaysOut:
    """No configuration object may carry or print key material."""

    def test_the_loaded_secret_never_appears_in_a_repr(self, tmp_path: Path) -> None:
        path = tmp_path / "hs.key"
        path.write_text("S" * 31 + "Z")
        config = JwtAuthConfig(secret_path=str(path), algorithms=("HS256",))

        assert "S" * 31 not in repr(config.keyring())

    def test_a_private_key_is_refused_in_public_keys(self) -> None:
        private = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEI\n-----END PRIVATE KEY-----"
        with pytest.raises(ConfigError, match="private key"):
            JwtAuthConfig(public_keys={"k1": private}, algorithms=("EdDSA",))


def _rsa_pem(bits: int) -> tuple[str, str]:
    """A private key of *bits* and its public PEM."""
    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415
    from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415

    key = rsa.generate_private_key(public_exponent=65537, key_size=bits)
    private = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    public = (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    return private, public


class TestRsaKeySize:
    """RFC 7518 §3.3: an RSA key below 2048 bits is refused on both sides."""

    def test_a_short_issuer_key_is_refused(self) -> None:
        private, _ = _rsa_pem(1024)

        with pytest.raises(ConfigError, match="1024-bit RSA"):
            _issuer(private, "RS256")

    def test_a_short_published_key_is_refused(self) -> None:
        _, public = _rsa_pem(1024)

        with pytest.raises(ConfigError, match="1024-bit RSA"):
            JwtAuthConfig(public_keys={"k1": public}, algorithms=("RS256",))

    def test_a_short_typed_published_key_is_refused(self) -> None:
        _, public = _rsa_pem(1024)

        with pytest.raises(ConfigError, match="1024-bit RSA"):
            JwtAuthConfig(public_keys={"k1": JwtPublicKey(alg="RS256", pem=public)})

    def test_a_2048_bit_key_is_accepted(self) -> None:
        _, public = _rsa_pem(2048)

        JwtAuthConfig(public_keys={"k1": public}, algorithms=("RS256",))


class TestIssuerFields:
    def test_a_blank_secret_is_refused(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            _issuer(" " * 40)

    def test_the_mandatory_fields_have_no_empty_default(self) -> None:
        with pytest.raises(TypeError):
            JwtIssuerConfig(signing_key=Secret("k" * 32))  # type: ignore[call-arg]

    def test_an_unknown_algorithm_is_named_before_the_key(self) -> None:
        with pytest.raises(ConfigError, match="algorithm"):
            _issuer("short", "HS999")


class TestPublicKeyFit:
    """A key that cannot verify with its algorithm fails at startup, not per request."""

    @staticmethod
    def _ed25519_public() -> str:
        from cryptography.hazmat.primitives import serialization  # noqa: PLC0415
        from cryptography.hazmat.primitives.asymmetric import ed25519  # noqa: PLC0415

        return (
            ed25519.Ed25519PrivateKey.generate()
            .public_key()
            .public_bytes(
                serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
            )
            .decode()
        )

    def test_a_key_of_another_type_than_its_alg_is_refused(self) -> None:
        with pytest.raises(ConfigError, match=r"is a OKP key; none of \['RS256'\]"):
            JwtAuthConfig(public_keys={"k1": JwtPublicKey(alg="RS256", pem=self._ed25519_public())})

    def test_a_pem_text_key_keeps_only_the_algorithms_of_its_type(self) -> None:
        """The released form shares one list: an RSA key never takes EdDSA, nor the reverse."""
        rsa_public = _rsa_pem(2048)[1]
        config = JwtAuthConfig(
            public_keys={"rsa": rsa_public, "ed": self._ed25519_public()},
            algorithms=("RS256", "PS256", "EdDSA"),
        )

        keys = config.keyring()
        assert keys.pick("rsa").algorithms == ("RS256", "PS256")  # type: ignore[union-attr]
        assert keys.pick("ed").algorithms == ("EdDSA",)  # type: ignore[union-attr]

    def test_a_pem_text_key_that_fits_no_algorithm_is_refused(self) -> None:
        with pytest.raises(ConfigError, match="is a OKP key; none of"):
            JwtAuthConfig(public_keys={"k1": self._ed25519_public()}, algorithms=("RS256",))

    def test_the_keyring_holds_the_keys_as_checked(self) -> None:
        """Validation runs when the keyring is built, so a dict changed after
        construction cannot slip a weak key past the RSA floor."""
        config = JwtAuthConfig(public_keys={"k1": _rsa_pem(2048)[1]}, algorithms=("RS256",))
        config.public_keys["k1"] = _rsa_pem(1024)[1]

        with pytest.raises(ConfigError, match="1024-bit RSA"):
            JwtAuthenticator(config)

    def test_a_curve_must_match_its_algorithm(self) -> None:
        from cryptography.hazmat.primitives import serialization  # noqa: PLC0415
        from cryptography.hazmat.primitives.asymmetric import ec  # noqa: PLC0415

        p384 = (
            ec.generate_private_key(ec.SECP384R1())
            .public_key()
            .public_bytes(
                serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
            )
            .decode()
        )
        with pytest.raises(ConfigError, match=r"is a P-384 key; none of \['ES256'\]"):
            JwtAuthConfig(public_keys={"k1": JwtPublicKey(alg="ES256", pem=p384)})
        JwtAuthConfig(public_keys={"k1": JwtPublicKey(alg="ES384", pem=p384)})

    def test_a_malformed_pem_is_refused_without_keeping_the_parser_error(self) -> None:
        broken = "-----BEGIN PUBLIC KEY-----\nnot base64\n-----END PUBLIC KEY-----"
        with pytest.raises(ConfigError, match="is not a PEM or OpenSSH public key") as refused:
            JwtAuthConfig(public_keys={"k1": JwtPublicKey(alg="EdDSA", pem=broken)})

        assert refused.value.__cause__ is None
        assert refused.value.__context__ is None


class TestReadErrorsKeepNothing:
    """``from None`` hides a cause but keeps ``__context__``: the bytes must not travel."""

    def test_an_undecodable_secret_file_keeps_no_context(self, tmp_path: Path) -> None:
        path = tmp_path / "hs.key"
        path.write_bytes(b"\xff\xfe-binary-secret-of-many-bytes-long-enough")
        config = JwtAuthConfig(secret_path=str(path), algorithms=("HS256",))

        with pytest.raises(ConfigError, match="Could not read") as refused:
            JwtAuthenticator(config)

        assert refused.value.__cause__ is None
        assert refused.value.__context__ is None

    def test_the_testing_helper_hides_its_secret(self) -> None:
        auth = jwt_test_auth()

        assert auth.secret not in repr(auth)


class TestPublicKeyForms:
    @staticmethod
    def _keypair() -> tuple[str, str]:
        from cryptography.hazmat.primitives import serialization  # noqa: PLC0415
        from cryptography.hazmat.primitives.asymmetric import ed25519  # noqa: PLC0415

        key = ed25519.Ed25519PrivateKey.generate()
        private = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode()
        public = (
            key.public_key()
            .public_bytes(
                serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
            )
            .decode()
        )
        return private, public

    def test_the_two_forms_never_mix(self) -> None:
        _, public = self._keypair()
        with pytest.raises(ConfigError, match="not both"):
            JwtAuthConfig(
                public_keys={"a": public, "b": JwtPublicKey(alg="EdDSA", pem=public)},
                algorithms=("EdDSA",),
            )

    def test_algorithms_beside_typed_keys_is_refused(self) -> None:
        _, public = self._keypair()
        with pytest.raises(ConfigError, match="remove it"):
            JwtAuthConfig(
                public_keys={"a": JwtPublicKey(alg="EdDSA", pem=public)}, algorithms=("EdDSA",)
            )

    def test_pem_text_keys_need_algorithms(self) -> None:
        _, public = self._keypair()
        with pytest.raises(ConfigError, match="non-empty 'algorithms'"):
            JwtAuthConfig(public_keys={"a": public})

    def test_a_public_key_never_takes_a_symmetric_algorithm(self) -> None:
        _, public = self._keypair()
        with pytest.raises(ConfigError, match="needs a 'secret'"):
            JwtPublicKey(alg="HS256", pem=public)

    def test_a_public_key_object_never_holds_private_material(self) -> None:
        private, _ = self._keypair()
        with pytest.raises(ConfigError, match="holds a private key"):
            JwtPublicKey(alg="EdDSA", pem=private)

    def test_the_typed_form_decodes_from_yaml_values(self) -> None:
        from loom.core.config.convert import convert_config  # noqa: PLC0415

        _, public = self._keypair()
        config = convert_config(
            {"public_keys": {"a": {"alg": "EdDSA", "pem": public}}}, JwtAuthConfig, strict=True
        )
        assert config.public_keys == {"a": JwtPublicKey(alg="EdDSA", pem=public)}

    @staticmethod
    def _openssh_rsa(bits: int) -> str:
        from cryptography.hazmat.primitives import serialization  # noqa: PLC0415
        from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415

        return (
            rsa.generate_private_key(public_exponent=65537, key_size=bits)
            .public_key()
            .public_bytes(serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH)
            .decode()
        )

    def test_an_openssh_rsa_key_cannot_slip_under_the_size_floor(self) -> None:
        """An OpenSSH key is parsed like a PEM one, so the 2048-bit floor holds."""
        ssh = self._openssh_rsa(1024)
        with pytest.raises(ConfigError, match="1024-bit RSA"):
            JwtAuthConfig(public_keys={"a": JwtPublicKey(alg="RS256", pem=ssh)})
        with pytest.raises(ConfigError, match="1024-bit RSA"):
            JwtAuthConfig(public_keys={"a": ssh}, algorithms=("RS256",))

    def test_a_released_openssh_public_key_still_loads(self) -> None:
        """The released verifier accepted OpenSSH lines through PyJWT; they still load."""
        keys = JwtAuthConfig(
            public_keys={"a": self._openssh_rsa(2048)}, algorithms=("RS256",)
        ).keyring()
        assert keys.pick("a").algorithms == ("RS256",)  # type: ignore[union-attr]

    @pytest.mark.parametrize("kid", [1, ["a"], {"k": "a"}])
    async def test_a_kid_that_is_not_a_string_matches_nothing(self, kid: object) -> None:
        private, public = self._keypair()
        authenticator = JwtAuthenticator(
            JwtAuthConfig(public_keys={"1": JwtPublicKey(alg="EdDSA", pem=public)})
        )
        signed = pyjwt.encode(
            {"sub": "ada", "exp": int(time.time()) + 60}, private, algorithm="EdDSA"
        )
        _, payload, signature = signed.split(".")
        header = json.dumps({"alg": "EdDSA", "typ": "JWT", "kid": kid}).encode()
        forged = base64.urlsafe_b64encode(header).rstrip(b"=").decode()

        assert (
            await authenticator.authenticate(_credentials(f"{forged}.{payload}.{signature}"))
            is None
        )


class TestSigningProbeKeepsNothing:
    def test_a_key_that_does_not_parse_keeps_no_parser_error(self) -> None:
        broken = "-----BEGIN PRIVATE KEY-----\nnot base64 at all\n-----END PRIVATE KEY-----"
        with pytest.raises(ConfigError, match="not an unencrypted PEM private key") as refused:
            _issuer(broken, "EdDSA")

        assert refused.value.__cause__ is None
        assert refused.value.__context__ is None


class TestKeyMaterialIsNeverASharedSecret:
    """Whoever holds a published key could sign HS tokens with it (RFC 8725 §2.1)."""

    @pytest.mark.parametrize(
        "material",
        [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl x",
            "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBP x",
            '{"kty": "RSA", "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbX", "e": "AQAB"}',
        ],
    )
    def test_an_openssh_key_or_a_jwk_is_refused(self, material: str) -> None:
        with pytest.raises(ConfigError, match="is an asymmetric key; HS256"):
            JwtAuthConfig(secret=Secret(material), algorithms=("HS256",))


class TestIssuerKeyFit:
    """The issuer applies the verifiers' type, curve and size rule to its key."""

    def test_a_curve_that_does_not_match_its_algorithm_is_refused(self) -> None:
        from cryptography.hazmat.primitives import serialization  # noqa: PLC0415
        from cryptography.hazmat.primitives.asymmetric import ec  # noqa: PLC0415

        p384 = (
            ec.generate_private_key(ec.SECP384R1())
            .private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
            .decode()
        )
        with pytest.raises(ConfigError, match=r"is a P-384 key; none of \['ES256'\]"):
            _issuer(p384, "ES256")

    def test_a_weak_rsa_key_is_refused(self) -> None:
        with pytest.raises(ConfigError, match="1024-bit RSA"):
            _issuer(_rsa_pem(1024)[0], "RS256")

    def test_the_issuer_holds_its_key_as_a_secret(self) -> None:
        from loom.rest.auth import JwtIssuer  # noqa: PLC0415

        private = _rsa_pem(2048)[0]
        issuer = JwtIssuer(_issuer(private, "RS256"))
        assert private not in repr(vars(issuer))
