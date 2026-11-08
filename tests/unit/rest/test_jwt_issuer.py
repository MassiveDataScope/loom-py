"""Tests for ``JwtIssuer``: the signing counterpart of ``JwtAuthenticator``."""

from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

import jwt as pyjwt
import msgspec
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa

from loom.core.config.convert import convert_config, dump_config
from loom.core.config.errors import ConfigError
from loom.core.identity import Identity
from loom.core.identity.issuer import IssuedToken, TokenIssuer
from loom.core.model.redaction import Secret
from loom.rest.auth import (
    JwtAuthConfig,
    JwtAuthenticator,
    JwtIssuer,
    JwtIssuerConfig,
    JwtPublicKey,
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


_SHARED_SECRET = "shared-secret-of-thirty-two-bytes"
_BROKEN_PEM = "-----BEGIN PRIVATE KEY-----\nnot base64 at all\n-----END PRIVATE KEY-----"


def _rsa_keypair() -> tuple[str, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
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


def _issuer_config(private: str, **overrides: Any) -> JwtIssuerConfig:
    params: dict[str, Any] = {
        "signing_key": Secret(private),
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


def test_the_issuer_satisfies_the_protocol() -> None:
    private, _ = _keypair()

    issuer: TokenIssuer = JwtIssuer(_issuer_config(private))

    assert callable(issuer.issue)


def test_issuing_returns_the_metadata_the_caller_would_otherwise_decode() -> None:
    """`-> str` would force the login endpoint to decode what it just signed."""
    private, _ = _keypair()
    before = time.time()

    issued = JwtIssuer(_issuer_config(private)).issue(_identity())

    assert isinstance(issued, IssuedToken)
    assert issued.token.count(".") == 2
    assert issued.jti
    assert before + 900 <= issued.expires_at.timestamp() <= time.time() + 900


def test_two_tokens_never_share_a_jti() -> None:
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(private))

    first = issuer.issue(_identity())
    second = issuer.issue(_identity())

    assert first.jti != second.jti


# ── el round-trip, que es el test que importa ───────────────────────────────


async def test_the_authenticator_recovers_exactly_what_the_issuer_put_in() -> None:
    """Without this, the attributes projection can break and nobody notices."""
    private, public = _keypair()
    issued = JwtIssuer(_issuer_config(private)).issue(_identity())
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


def test_the_config_never_shows_key_material() -> None:
    """The key is a ``Secret``: a repr and a dump mask it, and an encoder that
    does not know the type refuses it instead of publishing it."""
    private, _ = _keypair()

    config = _issuer_config(private)

    assert "PRIVATE KEY" not in repr(config)
    assert dump_config(config)["signing_key"] == "***"
    with pytest.raises(TypeError):
        msgspec.json.encode(config)
    with pytest.raises(TypeError):
        msgspec.to_builtins(config)


def test_a_signing_key_given_as_plain_text_is_refused() -> None:
    private, _ = _keypair()
    with pytest.raises(TypeError, match="signing_key must be Secret"):
        _issuer_config(private, signing_key=private)


# ── invariantes que el emisor no puede dejar pasar ──────────────────────────


def test_an_attribute_named_like_a_registered_claim_cannot_overwrite_it() -> None:
    """The namespace makes rejection unnecessary: it lands as attr_sub, harmless."""
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(private))

    issued = issuer.issue(_identity(attributes={"sub": "someone-else"}))
    claims = pyjwt.decode(issued.token, options={"verify_signature": False}, audience=_AUDIENCE)

    assert claims["sub"] == "ada@example.com"
    assert claims[f"{ATTRIBUTE_CLAIM_PREFIX}sub"] == "someone-else"


async def test_an_attribute_named_like_the_roles_claim_grants_nothing() -> None:
    private, public = _keypair()
    issued = JwtIssuer(_issuer_config(private)).issue(
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


def test_an_identity_without_roles_is_a_translation_bug_not_a_caller() -> None:
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(private))

    roleless = _identity(roles=())

    with pytest.raises(ValueError, match="roles"):
        issuer.issue(roleless)


def test_an_anonymous_identity_is_rejected() -> None:
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(private))

    anonymous = _identity(subject="")

    with pytest.raises(ValueError, match="subject"):
        issuer.issue(anonymous)


def test_a_per_call_ttl_cannot_outlive_the_configured_one() -> None:
    """The configured ttl is the ceiling: no new field needed to bound it."""
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(private, ttl_seconds=900))

    shorter = issuer.issue(_identity(), ttl=timedelta(seconds=60))
    assert shorter.expires_at.timestamp() <= time.time() + 60 + 1

    identity = _identity()
    a_year = timedelta(days=365)

    with pytest.raises(ValueError, match="ttl"):
        issuer.issue(identity, ttl=a_year)


# ── configuración ───────────────────────────────────────────────────────────


def test_symmetric_signing_takes_a_shared_secret() -> None:
    config = _issuer_config(_SHARED_SECRET, algorithm="HS256")

    assert config.algorithm == "HS256"


@pytest.mark.parametrize("algorithm", ["none", "NONE"])
def test_the_none_algorithm_is_always_forbidden(algorithm: str) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError):
        _issuer_config(private, algorithm=algorithm)


def test_a_private_key_cannot_sign_with_a_symmetric_algorithm() -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="is a PEM key; HS256 needs a shared secret"):
        _issuer_config(private, algorithm="HS256")


def test_a_shared_secret_cannot_sign_with_an_asymmetric_algorithm() -> None:
    with pytest.raises(ConfigError, match="EdDSA needs a PEM private key"):
        _issuer_config(_SHARED_SECRET, algorithm="EdDSA")


def test_a_public_key_cannot_sign() -> None:
    _, public = _keypair()
    with pytest.raises(ConfigError, match="EdDSA needs a PEM private key"):
        _issuer_config(public)


def test_the_signing_key_decodes_from_resolved_text() -> None:
    """Bound from configuration, a placeholder resolves the key into a ``Secret``."""
    private, _ = _keypair()
    config = convert_config(
        {
            "signing_key": private,
            "algorithm": "EdDSA",
            "audience": _AUDIENCE,
            "issuer": _ISSUER,
            "roles_claim": _ROLES_CLAIM,
        },
        JwtIssuerConfig,
        strict=False,
    )
    assert isinstance(config.signing_key, Secret)
    assert config.signing_key.reveal() == private


@pytest.mark.parametrize("field", ["audience", "issuer"])
def test_audience_and_issuer_are_mandatory_when_issuing(field: str) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match=field):
        _issuer_config(private, **{field: "  "})


# ── rotación de claves ──────────────────────────────────────────────────────


def _credentials(token: str) -> RequestCredentials:
    return RequestCredentials(
        headers={"authorization": f"Bearer {token}"}, path="/", client_host=None
    )


async def test_a_token_signed_with_the_previous_key_still_verifies() -> None:
    """Rotation without an overlap window cuts every live session."""
    old_private, old_public = _keypair()
    new_private, new_public = _keypair()
    old_token = JwtIssuer(_issuer_config(old_private, kid="2026-07")).issue(_identity())
    new_token = JwtIssuer(_issuer_config(new_private, kid="2026-08")).issue(_identity())
    authenticator = JwtAuthenticator(
        JwtAuthConfig(
            public_keys={"2026-07": old_public, "2026-08": new_public},
            algorithms=("EdDSA",),
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )
    )

    assert await authenticator.authenticate(_credentials(old_token.token)) is not None
    assert await authenticator.authenticate(_credentials(new_token.token)) is not None


async def test_a_rotation_can_change_the_key_type() -> None:
    """Each key names its algorithm, so RSA and EdDSA keys overlap under two kids."""
    rsa_private, rsa_public = _rsa_keypair()
    ed_private, ed_public = _keypair()
    rsa_token = JwtIssuer(_issuer_config(rsa_private, algorithm="RS256", kid="rsa")).issue(
        _identity()
    )
    ed_token = JwtIssuer(_issuer_config(ed_private, kid="ed")).issue(_identity())
    authenticator = JwtAuthenticator(
        JwtAuthConfig(
            public_keys={
                "rsa": JwtPublicKey(alg="RS256", pem=rsa_public),
                "ed": JwtPublicKey(alg="EdDSA", pem=ed_public),
            },
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )
    )

    assert await authenticator.authenticate(_credentials(rsa_token.token)) is not None
    assert await authenticator.authenticate(_credentials(ed_token.token)) is not None


async def test_a_key_verifies_only_its_own_algorithm() -> None:
    """RFC 8725 section 3.1: a PS256 token is refused by a key published for RS256."""
    rsa_private, rsa_public = _rsa_keypair()
    ps_token = JwtIssuer(_issuer_config(rsa_private, algorithm="PS256", kid="rsa")).issue(
        _identity()
    )
    authenticator = JwtAuthenticator(
        JwtAuthConfig(
            public_keys={"rsa": JwtPublicKey(alg="RS256", pem=rsa_public)},
            audience=_AUDIENCE,
            issuer=_ISSUER,
            roles_claim=_ROLES_CLAIM,
        )
    )

    assert await authenticator.authenticate(_credentials(ps_token.token)) is None


async def test_an_unknown_kid_is_rejected_without_trying_every_key() -> None:
    """Try-all-keys would break the algorithm-to-key-family binding."""
    private, _ = _keypair()
    _, other_public = _keypair()
    token = JwtIssuer(_issuer_config(private, kid="unregistered")).issue(_identity())
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


def test_the_issued_token_carries_its_kid_in_the_header() -> None:
    private, _ = _keypair()

    issued = JwtIssuer(_issuer_config(private)).issue(_identity())

    assert pyjwt.get_unverified_header(issued.token)["kid"] == _KID


# ── inyección de claims por atributos ───────────────────────────────────────


def test_an_attribute_can_never_become_a_bare_claim() -> None:
    """The issuer cannot know the verifier's roles claim, so no attribute may
    land as a top-level name: prefixing makes the injection impossible."""
    private, _ = _keypair()
    issued = JwtIssuer(_issuer_config(private)).issue(_identity(attributes={"store": "madrid"}))

    claims = pyjwt.decode(issued.token, options={"verify_signature": False}, audience=_AUDIENCE)

    assert "store" not in claims
    assert claims[f"{ATTRIBUTE_CLAIM_PREFIX}store"] == "madrid"


async def test_a_roles_claim_cannot_be_forged_through_attributes() -> None:
    """The escalation this closes: issuer without a roles claim, verifier with one."""
    private, public = _keypair()
    issuer = JwtIssuer(_issuer_config(private, roles_claim="other_claim"))
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


def test_a_non_string_attribute_is_refused() -> None:
    """A list value would encode as an arbitrary set of roles."""
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(private))

    listed = _identity(attributes={"store": ["madrid", "role_data_developer"]})

    with pytest.raises(ValueError, match="string"):
        issuer.issue(listed)


def test_an_already_prefixed_attribute_is_refused() -> None:
    private, _ = _keypair()
    issuer = JwtIssuer(_issuer_config(private))

    prefixed = _identity(attributes={f"{ATTRIBUTE_CLAIM_PREFIX}store": "madrid"})

    with pytest.raises(ValueError, match=ATTRIBUTE_CLAIM_PREFIX):
        issuer.issue(prefixed)


# ── el secreto simétrico tampoco puede vivir en el struct ───────────────────


def test_a_symmetric_secret_is_shown_by_no_serializer() -> None:
    config = _issuer_config(_SHARED_SECRET, algorithm="HS256")

    assert _SHARED_SECRET not in repr(config)
    assert dump_config(config)["signing_key"] == "***"
    assert config.signing_key.reveal() == _SHARED_SECRET


# ── configuración: fail-fast al arrancar, no en la primera petición ─────────


def test_a_roles_claim_is_mandatory_for_the_issuer() -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="roles_claim"):
        _issuer_config(private, roles_claim=None)


@pytest.mark.parametrize("algorithm", [" HS256", "RS257", "not-an-algorithm"])
def test_an_unsupported_algorithm_fails_at_startup(algorithm: str) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="algorithm"):
        _issuer_config(private, algorithm=algorithm)


def test_a_lifetime_above_the_ceiling_is_refused() -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="ttl_seconds"):
        _issuer_config(private, ttl_seconds=10**9)


def test_a_blank_kid_is_refused() -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="kid"):
        _issuer_config(private, kid="  ")


async def test_mutating_the_config_cannot_widen_what_the_authenticator_accepts() -> None:
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
    foreign = JwtIssuer(_issuer_config(private, kid="injected")).issue(_identity())

    config.public_keys["injected"] = foreign_public

    assert (
        await authenticator.authenticate(
            RequestCredentials(
                headers={"authorization": f"Bearer {foreign.token}"}, path="/", client_host=None
            )
        )
        is None
    )


def test_the_verification_config_stays_serializable() -> None:
    """A config a dump cannot encode breaks every diagnostic that reports it."""
    _, public = _keypair()
    config = JwtAuthConfig(
        public_keys={_KID: public}, algorithms=("EdDSA",), audience=_AUDIENCE, issuer=_ISSUER
    )

    assert msgspec.json.encode(config)
    assert msgspec.to_builtins(config)


# ── huecos de configuración que fallaban tarde ──────────────────────────────


@pytest.mark.parametrize("ttl_seconds", [0, -1])
def test_a_non_positive_lifetime_is_refused(ttl_seconds: int) -> None:
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="ttl_seconds"):
        _issuer_config(private, ttl_seconds=ttl_seconds)


@pytest.mark.parametrize("claim", ["sub", "exp", "aud", "jti"])
def test_a_roles_claim_that_shadows_a_registered_claim_is_refused(claim: str) -> None:
    """Insertion order would silently drop the roles and raise nothing."""
    private, _ = _keypair()
    with pytest.raises(ConfigError, match="registered claim"):
        _issuer_config(private, roles_claim=claim)


async def test_a_token_without_a_kid_is_rejected_when_several_keys_exist() -> None:
    private, public = _keypair()
    _, other_public = _keypair()
    issued = JwtIssuer(_issuer_config(private, kid=None)).issue(_identity())
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


def test_a_malformed_key_fails_at_construction_not_at_the_first_login() -> None:
    """Reading the key is not parsing it, and the difference is user-visible.

    A malformed PEM raises a plain ``ValueError`` from ``cryptography`` -- not a
    ``PyJWTError`` -- so before the construction-time probe it escaped the signing
    handler and reached the caller as the ``ValueError`` the port documents as
    "this identity cannot be represented". A broken deployment then looked like a
    bad login, and only on the first one.
    """
    config = _issuer_config(_BROKEN_PEM)

    with pytest.raises(ConfigError, match="EdDSA"):
        JwtIssuer(config)


def test_the_construction_probe_reports_an_algorithm_the_key_cannot_serve() -> None:
    """A valid key with the wrong algorithm is the likelier operator mistake."""
    private, _ = _keypair()
    config = _issuer_config(private, algorithm="RS256")

    with pytest.raises(ConfigError, match="RS256"):
        JwtIssuer(config)


# ---------------------------------------------------------------------------
# Verifier derived from the signing key
# ---------------------------------------------------------------------------


class TestForIssuer:
    def test_derives_the_public_key_and_shares_every_matching_value(self) -> None:
        private, public = _keypair()
        issuer = _issuer_config(private, typ="at+jwt")
        config = JwtAuthConfig.for_issuer(issuer)
        assert config.public_keys == {_KID: JwtPublicKey(alg="EdDSA", pem=public)}
        assert config.algorithms == ()
        assert (config.audience, config.issuer, config.roles_claim) == (
            _AUDIENCE,
            _ISSUER,
            _ROLES_CLAIM,
        )
        assert config.accepted_types == ("at+jwt",)

    async def test_verifies_what_the_issuer_mints(self) -> None:
        private, _ = _keypair()
        issuer_config = _issuer_config(private, typ="at+jwt")
        issued = JwtIssuer(issuer_config).issue(_identity())
        verifier = JwtAuthenticator(JwtAuthConfig.for_issuer(issuer_config))
        recovered = await verifier.authenticate(
            RequestCredentials(
                headers={"authorization": f"Bearer {issued.token}"}, path="/", client_host=None
            )
        )
        assert recovered is not None
        assert recovered.subject == "ada@example.com"

    def test_an_asymmetric_issuer_needs_a_kid(self) -> None:
        private, _ = _keypair()
        with pytest.raises(ConfigError, match="kid"):
            JwtAuthConfig.for_issuer(_issuer_config(private, kid=None))

    def test_previous_key_stays_published(self) -> None:
        private, public = _keypair()
        _, previous_public = _keypair()
        issuer = _issuer_config(private, kid="2026-09")
        previous = JwtPublicKey(alg="EdDSA", pem=previous_public)
        config = JwtAuthConfig.for_issuer(issuer, previous_keys={"2026-08": previous})
        assert config.public_keys == {
            "2026-09": JwtPublicKey(alg="EdDSA", pem=public),
            "2026-08": previous,
        }

    def test_private_material_is_not_retained(self) -> None:
        private, _ = _keypair()
        config = JwtAuthConfig.for_issuer(_issuer_config(private))
        private_body = private.splitlines()[1]
        assert private_body not in repr(config)
        assert private_body.encode() not in msgspec.json.encode(config)

    def test_unparseable_key_chains_the_reason(self) -> None:
        issuer = _issuer_config(_BROKEN_PEM)
        with pytest.raises(ConfigError, match="derive") as excinfo:
            JwtAuthConfig.for_issuer(issuer)
        assert excinfo.value.__cause__ is not None

    def test_reusing_the_issuer_kid_is_refused(self) -> None:
        private, _ = _keypair()
        _, other_public = _keypair()
        issuer = _issuer_config(private)
        with pytest.raises(ConfigError, match="reuses the issuer kid"):
            JwtAuthConfig.for_issuer(
                issuer, previous_keys={_KID: JwtPublicKey(alg="EdDSA", pem=other_public)}
            )

    def test_a_symmetric_issuer_shares_its_secret(self) -> None:
        issuer = _issuer_config(_SHARED_SECRET, algorithm="HS256")
        config = JwtAuthConfig.for_issuer(issuer)
        assert config.secret is issuer.signing_key
        assert config.algorithms == ("HS256",)

    def test_previous_keys_are_refused_for_a_symmetric_issuer(self) -> None:
        _, previous_public = _keypair()
        issuer = _issuer_config(_SHARED_SECRET, algorithm="HS256")
        previous = {"old": JwtPublicKey(alg="EdDSA", pem=previous_public)}
        with pytest.raises(ConfigError, match="asymmetric issuer only"):
            JwtAuthConfig.for_issuer(issuer, previous_keys=previous)
