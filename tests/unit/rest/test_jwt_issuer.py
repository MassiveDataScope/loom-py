"""Tests for ``JwtIssuer``: the signing counterpart of ``JwtAuthenticator``."""

from __future__ import annotations

import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import jwt as pyjwt
import msgspec
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from loom.core.config.errors import ConfigError
from loom.core.identity import Identity
from loom.core.identity.issuer import IssuedToken, TokenIssuer
from loom.rest.auth import (
    JwtAuthConfig,
    JwtAuthenticator,
    JwtIssuer,
    JwtIssuerConfig,
    ManagedPrivateKey,
    PrivateKeyFile,
    SharedSecretFile,
)
from loom.rest.auth.abc import RequestCredentials
from loom.rest.auth.jwt import ATTRIBUTE_CLAIM_PREFIX

_AUDIENCE = "unit-test-api"
_ISSUER = "unit-test-gateway"
_ROLES_CLAIM = "loom_sql_roles"
_KID = "2026-08"


def _keypair() -> tuple[str, str]:
    key = ed25519.Ed25519PrivateKey.generate()
    private = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    public = (
        key.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return private, public


def _issuer_config(tmp_path: Path, private: str, **overrides: Any) -> JwtIssuerConfig:
    tmp_path.mkdir(parents=True, exist_ok=True)
    key_file = tmp_path / "signing.pem"
    key_file.write_text(private)
    params: dict[str, Any] = {
        "signing_key": PrivateKeyFile(str(key_file)),
        "algorithm": "EdDSA",
        "audience": _AUDIENCE,
        "issuer": _ISSUER,
        "roles_claim": _ROLES_CLAIM,
        "ttl_seconds": 900,
        "kid": _KID,
    }
    params.update(overrides)
    return JwtIssuerConfig(**params)


def _identity(**overrides: Any) -> Identity:
    params: dict[str, Any] = {
        "subject": "ada@example.com",
        "roles": ("role_biz_sales", "role_biz_market"),
        "attributes": {"store": "madrid"},
        "mechanism": "jwt",
    }
    params.update(overrides)
    return Identity(**params)


# ── el contrato del emisor ──────────────────────────────────────────────────


def test_the_issuer_satisfies_the_protocol(tmp_path: Path) -> None:
    private, _ = _keypair()

    issuer: TokenIssuer = JwtIssuer(_issuer_config(tmp_path, private))

    assert callable(issuer.issue)


def test_issuing_returns_the_metadata_the_caller_would_otherwise_decode(tmp_path: Path) -> None:
    """`-> str` would force the login endpoint to decode what it just signed."""
    private, _ = _keypair()
    before = time.time()

    issued = JwtIssuer(_issuer_config(tmp_path, private)).issue(_identity())

    assert isinstance(issued, IssuedToken)
    assert issued.token.count(".") == 2
    assert issued.jti
    assert before + 900 <= issued.expires_at.timestamp() <= time.time() + 900


def test_two_tokens_never_share_a_jti(tmp_path: Path) -> None:
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(tmp_path, private))

    first = issuer.issue(_identity())
    second = issuer.issue(_identity())

    assert first.jti != second.jti


# ── el round-trip, que es el test que importa ───────────────────────────────


async def test_the_authenticator_recovers_exactly_what_the_issuer_put_in(tmp_path: Path) -> None:
    """Without this, the attributes projection can break and nobody notices."""
    private, public = _keypair()
    issued = JwtIssuer(_issuer_config(tmp_path, private)).issue(_identity())
    authenticator = JwtAuthenticator(
        JwtAuthConfig(
            public_keys={_KID: public},
            algorithms=("EdDSA",),
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )
    )

    recovered = await authenticator.authenticate(
        RequestCredentials(
            headers={"authorization": f"Bearer {issued.token}"}, path="/", client_host=None
        )
    )

    assert recovered is not None
    assert recovered.subject == "ada@example.com"
    assert recovered.roles == ("role_biz_sales", "role_biz_market")
    assert recovered.attributes["store"] == "madrid"


# ── material de clave: msgspec se salta __repr__ ────────────────────────────


def test_the_config_never_carries_key_material(tmp_path: Path) -> None:
    """`__repr__` redaction is cosmetic: msgspec encodes the struct fields."""
    private, _ = _keypair()

    config = _issuer_config(tmp_path, private)
    encoded = msgspec.json.encode(config)

    assert b"PRIVATE KEY" not in encoded
    assert b"PRIVATE KEY" not in repr(config).encode()
    assert b"PRIVATE KEY" not in msgspec.json.encode(msgspec.to_builtins(config))


# ── invariantes que el emisor no puede dejar pasar ──────────────────────────


def test_an_attribute_named_like_a_registered_claim_cannot_overwrite_it(tmp_path: Path) -> None:
    """The namespace makes rejection unnecessary: it lands as attr_sub, harmless."""
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(tmp_path, private))

    issued = issuer.issue(_identity(attributes={"sub": "someone-else"}))
    claims = pyjwt.decode(issued.token, options={"verify_signature": False}, audience=_AUDIENCE)

    assert claims["sub"] == "ada@example.com"
    assert claims[f"{ATTRIBUTE_CLAIM_PREFIX}sub"] == "someone-else"


async def test_an_attribute_named_like_the_roles_claim_grants_nothing(tmp_path: Path) -> None:
    private, public = _keypair()
    issued = JwtIssuer(_issuer_config(tmp_path, private)).issue(
        _identity(roles=("role_biz_sales",), attributes={_ROLES_CLAIM: "role_data_developer"})
    )
    authenticator = JwtAuthenticator(
        JwtAuthConfig(
            public_keys={_KID: public},
            algorithms=("EdDSA",),
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )
    )

    recovered = await authenticator.authenticate(
        RequestCredentials(
            headers={"authorization": f"Bearer {issued.token}"}, path="/", client_host=None
        )
    )

    assert recovered is not None
    assert recovered.roles == ("role_biz_sales",)


def test_an_identity_without_roles_is_a_translation_bug_not_a_caller(tmp_path: Path) -> None:
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(tmp_path, private))

    roleless = _identity(roles=())

    with pytest.raises(ValueError, match="roles"):
        issuer.issue(roleless)


def test_an_anonymous_identity_is_rejected(tmp_path: Path) -> None:
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(tmp_path, private))

    anonymous = _identity(subject="")

    with pytest.raises(ValueError, match="subject"):
        issuer.issue(anonymous)


def test_a_per_call_ttl_cannot_outlive_the_configured_one(tmp_path: Path) -> None:
    """The configured ttl is the ceiling: no new field needed to bound it."""
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(tmp_path, private, ttl_seconds=900))

    shorter = issuer.issue(_identity(), ttl=timedelta(seconds=60))
    assert shorter.expires_at.timestamp() <= time.time() + 60 + 1

    identity = _identity()
    a_year = timedelta(days=365)

    with pytest.raises(ValueError, match="ttl"):
        issuer.issue(identity, ttl=a_year)


# ── configuración ───────────────────────────────────────────────────────────


def test_symmetric_signing_is_chosen_by_naming_a_shared_secret(tmp_path: Path) -> None:
    secret_file = tmp_path / "hs.key"
    secret_file.write_text("shared-secret-of-thirty-two-bytes")
    config = JwtIssuerConfig(
        signing_key=SharedSecretFile(str(secret_file)),
        algorithm="HS256",
        audience=_AUDIENCE,
        issuer=_ISSUER,
        roles_claim=_ROLES_CLAIM,
    )

    assert config.algorithm == "HS256"


@pytest.mark.parametrize("algorithm", ["none", "NONE"])
def test_the_none_algorithm_is_always_forbidden(tmp_path: Path, algorithm: str) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError):
        _issuer_config(tmp_path, private, algorithm=algorithm)


def test_a_private_key_cannot_sign_with_a_symmetric_algorithm(tmp_path: Path) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="SharedSecretFile"):
        _issuer_config(tmp_path, private, algorithm="HS256")


def test_a_shared_secret_cannot_sign_with_an_asymmetric_algorithm(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not a SharedSecretFile"):
        JwtIssuerConfig(
            signing_key=SharedSecretFile(str(tmp_path / "hs.key")),
            algorithm="EdDSA",
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )


def test_the_signing_key_decodes_from_a_typed_mapping() -> None:
    """Each case names itself with ``type`` and carries only its own settings."""
    config = msgspec.convert(
        {
            "signing_key": {
                "type": "managed_private_key",
                "ref": "secrets:/myapp/jwt",
                "region": "eu-west-1",
            },
            "algorithm": "EdDSA",
            "audience": _AUDIENCE,
            "issuer": _ISSUER,
            "roles_claim": _ROLES_CLAIM,
        },
        JwtIssuerConfig,
    )
    assert config.signing_key == ManagedPrivateKey("secrets:/myapp/jwt", region="eu-west-1")


def test_a_setting_of_another_key_case_is_refused() -> None:
    with pytest.raises(msgspec.ValidationError, match="region"):
        msgspec.convert(
            {"type": "private_key_file", "path": "/k.pem", "region": "eu-west-1"},
            PrivateKeyFile,
        )


@pytest.mark.parametrize("field", ["audience", "issuer"])
def test_audience_and_issuer_are_mandatory_when_issuing(tmp_path: Path, field: str) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match=field):
        _issuer_config(tmp_path, private, **{field: "  "})


# ── rotación de claves ──────────────────────────────────────────────────────


async def test_a_token_signed_with_the_previous_key_still_verifies(tmp_path: Path) -> None:
    """Rotation without an overlap window cuts every live session."""
    old_private, old_public = _keypair()
    new_private, new_public = _keypair()
    old_token = JwtIssuer(_issuer_config(tmp_path / "old", old_private, kid="2026-07")).issue(
        _identity()
    )
    new_token = JwtIssuer(_issuer_config(tmp_path / "new", new_private, kid="2026-08")).issue(
        _identity()
    )
    authenticator = JwtAuthenticator(
        JwtAuthConfig(
            public_keys={"2026-07": old_public, "2026-08": new_public},
            algorithms=("EdDSA",),
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )
    )
    creds = lambda token: RequestCredentials(  # noqa: E731
        headers={"authorization": f"Bearer {token}"}, path="/", client_host=None
    )

    assert await authenticator.authenticate(creds(old_token.token)) is not None
    assert await authenticator.authenticate(creds(new_token.token)) is not None


async def test_an_unknown_kid_is_rejected_without_trying_every_key(tmp_path: Path) -> None:
    """Try-all-keys would break the algorithm-to-key-family binding."""
    private, _ = _keypair()
    _, other_public = _keypair()
    token = JwtIssuer(_issuer_config(tmp_path, private, kid="unregistered")).issue(_identity())
    authenticator = JwtAuthenticator(
        JwtAuthConfig(
            public_keys={"2026-08": other_public},
            algorithms=("EdDSA",),
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )
    )

    assert (
        await authenticator.authenticate(
            RequestCredentials(
                headers={"authorization": f"Bearer {token.token}"}, path="/", client_host=None
            )
        )
        is None
    )


def test_the_issued_token_carries_its_kid_in_the_header(tmp_path: Path) -> None:
    private, _ = _keypair()

    issued = JwtIssuer(_issuer_config(tmp_path, private)).issue(_identity())

    assert pyjwt.get_unverified_header(issued.token)["kid"] == _KID


# ── inyección de claims por atributos ───────────────────────────────────────


def test_an_attribute_can_never_become_a_bare_claim(tmp_path: Path) -> None:
    """The issuer cannot know the verifier's roles claim, so no attribute may
    land as a top-level name: prefixing makes the injection impossible."""
    private, _ = _keypair()
    issued = JwtIssuer(_issuer_config(tmp_path, private)).issue(
        _identity(attributes={"store": "madrid"})
    )

    claims = pyjwt.decode(issued.token, options={"verify_signature": False}, audience=_AUDIENCE)

    assert "store" not in claims
    assert claims[f"{ATTRIBUTE_CLAIM_PREFIX}store"] == "madrid"


async def test_a_roles_claim_cannot_be_forged_through_attributes(tmp_path: Path) -> None:
    """The escalation this closes: issuer without a roles claim, verifier with one."""
    private, public = _keypair()
    issuer = JwtIssuer(_issuer_config(tmp_path, private, roles_claim="other_claim"))
    issued = issuer.issue(_identity(attributes={_ROLES_CLAIM: "role_data_developer"}))
    authenticator = JwtAuthenticator(
        JwtAuthConfig(
            public_keys={_KID: public},
            algorithms=("EdDSA",),
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )
    )

    recovered = await authenticator.authenticate(
        RequestCredentials(
            headers={"authorization": f"Bearer {issued.token}"}, path="/", client_host=None
        )
    )

    assert recovered is not None
    assert recovered.roles == ()


def test_a_non_string_attribute_is_refused(tmp_path: Path) -> None:
    """A list value would encode as an arbitrary set of roles."""
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(tmp_path, private))

    listed = _identity(attributes={"store": ["madrid", "role_data_developer"]})

    with pytest.raises(ValueError, match="string"):
        issuer.issue(listed)


def test_an_already_prefixed_attribute_is_refused(tmp_path: Path) -> None:
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(tmp_path, private))

    prefixed = _identity(attributes={f"{ATTRIBUTE_CLAIM_PREFIX}store": "madrid"})

    with pytest.raises(ValueError, match=ATTRIBUTE_CLAIM_PREFIX):
        issuer.issue(prefixed)


# ── el secreto simétrico tampoco puede vivir en el struct ───────────────────


def test_a_symmetric_secret_never_carries_into_the_struct(tmp_path: Path) -> None:
    secret_file = tmp_path / "hs.key"
    secret_file.write_text("SHARED-SIGNING-SECRET-OF-32-BYTES")

    config = JwtIssuerConfig(
        signing_key=SharedSecretFile(str(secret_file)),
        algorithm="HS256",
        audience=_AUDIENCE,
        issuer=_ISSUER,
        roles_claim=_ROLES_CLAIM,
    )

    assert b"SHARED-SIGNING-SECRET-OF-32-BYTES" not in msgspec.json.encode(config)
    assert config.load_signing_key() == "SHARED-SIGNING-SECRET-OF-32-BYTES"


# ── configuración: fail-fast al arrancar, no en la primera petición ─────────


def test_a_roles_claim_is_mandatory_for_the_issuer(tmp_path: Path) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="roles_claim"):
        _issuer_config(tmp_path, private, roles_claim=None)


@pytest.mark.parametrize("algorithm", [" HS256", "RS257", "not-an-algorithm"])
def test_an_unsupported_algorithm_fails_at_startup(tmp_path: Path, algorithm: str) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="algorithm"):
        _issuer_config(tmp_path, private, algorithm=algorithm)


def test_a_lifetime_above_the_ceiling_is_refused(tmp_path: Path) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="ttl_seconds"):
        _issuer_config(tmp_path, private, ttl_seconds=10**9)


def test_a_blank_kid_is_refused(tmp_path: Path) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="kid"):
        _issuer_config(tmp_path, private, kid="  ")


def test_a_binary_key_file_fails_as_a_config_error(tmp_path: Path) -> None:
    """UnicodeDecodeError carries the file contents on the exception object."""
    key_file = tmp_path / "signing.pem"
    key_file.write_bytes(b"\x30\x82\x01\x22\xff\xfe")

    config = JwtIssuerConfig(
        signing_key=PrivateKeyFile(str(key_file)),
        algorithm="EdDSA",
        audience=_AUDIENCE,
        issuer=_ISSUER,
        roles_claim=_ROLES_CLAIM,
    )

    with pytest.raises(ConfigError):
        config.load_signing_key()


async def test_mutating_the_config_cannot_widen_what_the_authenticator_accepts(
    tmp_path: Path,
) -> None:
    """The allowlist is copied on construction: frozen stops rebinding, not mutation."""
    private, public = _keypair()
    _, foreign_public = _keypair()
    config = JwtAuthConfig(
        public_keys={_KID: public},
        algorithms=("EdDSA",),
        audience=_AUDIENCE,
        issuer=_ISSUER,
        roles_claim=_ROLES_CLAIM,
    )
    authenticator = JwtAuthenticator(config)
    foreign = JwtIssuer(_issuer_config(tmp_path, private, kid="injected")).issue(_identity())

    config.public_keys["injected"] = foreign_public

    assert (
        await authenticator.authenticate(
            RequestCredentials(
                headers={"authorization": f"Bearer {foreign.token}"}, path="/", client_host=None
            )
        )
        is None
    )


def test_the_verification_config_stays_serializable(tmp_path: Path) -> None:
    """A config a dump cannot encode breaks every diagnostic that reports it."""
    _, public = _keypair()
    config = JwtAuthConfig(
        public_keys={_KID: public}, algorithms=("EdDSA",), audience=_AUDIENCE, issuer=_ISSUER
    )

    assert msgspec.json.encode(config)
    assert msgspec.to_builtins(config)


# ── huecos de configuración que fallaban tarde ──────────────────────────────


def test_a_missing_key_file_fails_as_a_config_error(tmp_path: Path) -> None:
    config = JwtIssuerConfig(
        signing_key=PrivateKeyFile(str(tmp_path / "absent.pem")),
        algorithm="EdDSA",
        audience=_AUDIENCE,
        issuer=_ISSUER,
        roles_claim=_ROLES_CLAIM,
    )

    with pytest.raises(ConfigError):
        config.load_signing_key()


@pytest.mark.parametrize("ttl_seconds", [0, -1])
def test_a_non_positive_lifetime_is_refused(tmp_path: Path, ttl_seconds: int) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="ttl_seconds"):
        _issuer_config(tmp_path, private, ttl_seconds=ttl_seconds)


@pytest.mark.parametrize("claim", ["sub", "exp", "aud", "jti"])
def test_a_roles_claim_that_shadows_a_registered_claim_is_refused(
    tmp_path: Path, claim: str
) -> None:
    """Insertion order would silently drop the roles and raise nothing."""
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="registered claim"):
        _issuer_config(tmp_path, private, roles_claim=claim)


async def test_a_token_without_a_kid_is_rejected_when_several_keys_exist(
    tmp_path: Path,
) -> None:
    private, public = _keypair()
    _, other_public = _keypair()
    issued = JwtIssuer(_issuer_config(tmp_path, private, kid=None)).issue(_identity())
    authenticator = JwtAuthenticator(
        JwtAuthConfig(
            public_keys={"2026-07": other_public, "2026-08": public},
            algorithms=("EdDSA",),
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )
    )

    assert (
        await authenticator.authenticate(
            RequestCredentials(
                headers={"authorization": f"Bearer {issued.token}"}, path="/", client_host=None
            )
        )
        is None
    )


def test_a_malformed_key_fails_at_construction_not_at_the_first_login(tmp_path: Path) -> None:
    """Reading the key is not parsing it, and the difference is user-visible.

    A malformed PEM raises a plain ``ValueError`` from ``cryptography`` -- not a
    ``PyJWTError`` -- so before the construction-time probe it escaped the signing
    handler and reached the caller as the ``ValueError`` the port documents as
    "this identity cannot be represented". A broken deployment then looked like a
    bad login, and only on the first one.
    """
    key_file = tmp_path / "broken.pem"
    key_file.write_text("-----BEGIN PRIVATE KEY-----\nnot base64 at all\n-----END PRIVATE KEY-----")
    config = JwtIssuerConfig(
        signing_key=PrivateKeyFile(str(key_file)),
        algorithm="EdDSA",
        audience=_AUDIENCE,
        issuer=_ISSUER,
        roles_claim=_ROLES_CLAIM,
    )

    with pytest.raises(ConfigError, match="EdDSA"):
        JwtIssuer(config)


def test_the_construction_probe_reports_an_algorithm_the_key_cannot_serve(tmp_path: Path) -> None:
    """A valid key with the wrong algorithm is the likelier operator mistake."""
    private, _ = _keypair()
    key_file = tmp_path / "ed.pem"
    key_file.write_text(private)
    config = JwtIssuerConfig(
        signing_key=PrivateKeyFile(str(key_file)),
        algorithm="RS256",
        audience=_AUDIENCE,
        issuer=_ISSUER,
        roles_claim=_ROLES_CLAIM,
    )

    with pytest.raises(ConfigError, match="RS256"):
        JwtIssuer(config)


# ---------------------------------------------------------------------------
# Signing key by managed-store reference
# ---------------------------------------------------------------------------


class _StaticResolver:
    def __init__(self, value: object) -> None:
        self._value = value
        self.asked: str | None = None

    def resolve(self, key: str) -> object:
        self.asked = key
        return self._value


def _ref_config(**overrides: Any) -> JwtIssuerConfig:
    params: dict[str, Any] = {
        "signing_key": ManagedPrivateKey("secrets:/myapp/jwt-signing-key"),
        "algorithm": "EdDSA",
        "audience": _AUDIENCE,
        "issuer": _ISSUER,
        "roles_claim": _ROLES_CLAIM,
        "kid": _KID,
    }
    params.update(overrides)
    return JwtIssuerConfig(**params)


def _patch_factory(monkeypatch: pytest.MonkeyPatch, resolver_factory: Any) -> None:
    from loom.rest.auth import config as auth_config

    monkeypatch.setitem(auth_config._KEY_REF_RESOLVER_FACTORIES, "secrets", resolver_factory)


class TestSigningKeyRef:
    @pytest.mark.parametrize("ref", ["vault:/myapp/key", "secrets:", "secrets", " :key"])
    def test_rejects_malformed_ref(self, ref: str) -> None:
        with pytest.raises(ConfigError, match="ManagedPrivateKey 'ref'"):
            ManagedPrivateKey(ref)

    def test_symmetric_algorithms_reject_a_managed_key(self) -> None:
        with pytest.raises(ConfigError, match="SharedSecretFile"):
            _ref_config(algorithm="HS256")

    def test_loads_key_through_resolver(self, monkeypatch: pytest.MonkeyPatch) -> None:
        private, _ = _keypair()
        resolver = _StaticResolver(private)
        _patch_factory(monkeypatch, lambda region: resolver)
        assert _ref_config().load_signing_key() == private
        assert resolver.asked == "/myapp/jwt-signing-key"

    def test_region_reaches_the_resolver(self, monkeypatch: pytest.MonkeyPatch) -> None:
        private, _ = _keypair()
        seen: list[str | None] = []

        def factory(region: str | None) -> _StaticResolver:
            seen.append(region)
            return _StaticResolver(private)

        _patch_factory(monkeypatch, factory)
        _ref_config(
            signing_key=ManagedPrivateKey("secrets:/myapp/jwt-signing-key", region="eu-west-1")
        ).load_signing_key()
        assert seen == ["eu-west-1"]

    @pytest.mark.parametrize("value", [{"pem": "not text"}, "", "   ", None])
    def test_non_text_resolution_is_refused(
        self, monkeypatch: pytest.MonkeyPatch, value: object
    ) -> None:
        _patch_factory(monkeypatch, lambda region: _StaticResolver(value))
        config = _ref_config()
        with pytest.raises(ConfigError, match="non-empty text"):
            config.load_signing_key()

    def test_resolver_config_errors_pass_through(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _Refusing:
            def resolve(self, key: str) -> object:
                raise ConfigError("boto3 is required")

        _patch_factory(monkeypatch, lambda region: _Refusing())
        config = _ref_config()
        with pytest.raises(ConfigError, match="boto3 is required"):
            config.load_signing_key()

    def test_resolver_failure_names_the_resolver(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _Failing:
            def resolve(self, key: str) -> object:
                raise RuntimeError("store unavailable")

        _patch_factory(monkeypatch, lambda region: _Failing())
        config = _ref_config()
        with pytest.raises(ConfigError, match="'secrets'") as excinfo:
            config.load_signing_key()
        assert isinstance(excinfo.value.__cause__, RuntimeError)


# ---------------------------------------------------------------------------
# Verifier derived from the signing key
# ---------------------------------------------------------------------------


class TestForIssuer:
    def test_derives_the_public_key_and_shares_every_matching_value(self, tmp_path: Path) -> None:
        private, public = _keypair()
        issuer = _issuer_config(tmp_path, private, typ="at+jwt")
        config = JwtAuthConfig.for_issuer(issuer)
        assert config.public_keys == {_KID: public}
        assert config.algorithms == ("EdDSA",)
        assert (config.audience, config.issuer, config.roles_claim) == (
            _AUDIENCE,
            _ISSUER,
            _ROLES_CLAIM,
        )
        assert config.accepted_types == ("at+jwt",)

    async def test_verifies_what_the_issuer_mints(self, tmp_path: Path) -> None:
        private, _ = _keypair()
        issuer_config = _issuer_config(tmp_path, private, typ="at+jwt")
        issued = JwtIssuer(issuer_config).issue(_identity())
        verifier = JwtAuthenticator(JwtAuthConfig.for_issuer(issuer_config))
        recovered = await verifier.authenticate(
            RequestCredentials(
                headers={"authorization": f"Bearer {issued.token}"}, path="/", client_host=None
            )
        )
        assert recovered is not None
        assert recovered.subject == "ada@example.com"

    def test_derives_from_a_managed_store_ref(self, monkeypatch: pytest.MonkeyPatch) -> None:
        private, public = _keypair()
        _patch_factory(monkeypatch, lambda region: _StaticResolver(private))
        config = JwtAuthConfig.for_issuer(_ref_config())
        assert config.public_keys == {_KID: public}

    def test_an_asymmetric_issuer_needs_a_kid(self, tmp_path: Path) -> None:
        private, _ = _keypair()
        with pytest.raises(ConfigError, match="kid"):
            JwtAuthConfig.for_issuer(_issuer_config(tmp_path, private, kid=None))

    def test_previous_key_stays_published(self, tmp_path: Path) -> None:
        private, public = _keypair()
        _, previous_public = _keypair()
        issuer = _issuer_config(tmp_path, private, kid="2026-09")
        config = JwtAuthConfig.for_issuer(issuer, previous_keys={"2026-08": previous_public})
        assert config.public_keys == {"2026-09": public, "2026-08": previous_public}

    def test_private_material_is_not_retained(self, tmp_path: Path) -> None:
        private, _ = _keypair()
        config = JwtAuthConfig.for_issuer(_issuer_config(tmp_path, private))
        private_body = private.splitlines()[1]
        assert private_body not in repr(config)
        assert private_body.encode() not in msgspec.json.encode(config)

    def test_unparseable_key_chains_the_reason(self, tmp_path: Path) -> None:
        issuer = _issuer_config(tmp_path, "not a pem")
        with pytest.raises(ConfigError, match="derive") as excinfo:
            JwtAuthConfig.for_issuer(issuer)
        assert excinfo.value.__cause__ is not None

    def test_reusing_the_issuer_kid_is_refused(self, tmp_path: Path) -> None:
        private, _ = _keypair()
        _, other_public = _keypair()
        issuer = _issuer_config(tmp_path, private)
        with pytest.raises(ConfigError, match="reuses the issuer kid"):
            JwtAuthConfig.for_issuer(issuer, previous_keys={_KID: other_public})

    def test_a_symmetric_issuer_shares_its_secret_file(self, tmp_path: Path) -> None:
        secret_file = tmp_path / "secret"
        secret_file.write_text("s" * 32)
        issuer = JwtIssuerConfig(
            signing_key=SharedSecretFile(str(secret_file)),
            algorithm="HS256",
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )
        config = JwtAuthConfig.for_issuer(issuer)
        assert config.secret_path == str(secret_file)
        assert config.algorithms == ("HS256",)

    def test_previous_keys_are_refused_for_a_symmetric_issuer(self, tmp_path: Path) -> None:
        secret_file = tmp_path / "secret"
        secret_file.write_text("s" * 32)
        _, previous_public = _keypair()
        issuer = JwtIssuerConfig(
            signing_key=SharedSecretFile(str(secret_file)),
            algorithm="HS256",
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )
        with pytest.raises(ConfigError, match="asymmetric issuer only"):
            JwtAuthConfig.for_issuer(issuer, previous_keys={"old": previous_public})
