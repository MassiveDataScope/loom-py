"""Tests for the native stateless ``JwtAuthMiddleware`` and its config (spec §3/§7.10)."""

from __future__ import annotations

import base64
import json
import pathlib
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable
from typing import Any

import jwt as pyjwt
import msgspec
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.identity import current_identity
from loom.rest.auth import JwtAuthConfig, JwtAuthenticator, JwtAuthMiddleware, RequestCredentials

_SECRET = "unit-test-secret"
# The secret is a path now: a msgspec struct emits its fields, so holding the
# value would publish the key that both verifies and signs.
_SECRET_PATH = str(pathlib.Path(tempfile.mkdtemp()) / "hs.key")
pathlib.Path(_SECRET_PATH).write_text(_SECRET)
_ROLES_CLAIM = "loom_roles"
_EMAIL = "ada@example.com"

_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]


async def _noop_asgi_app(scope: _Scope, receive: _Receive, send: _Send) -> None:
    """Minimal ASGI app used only as a wrapping target."""


def _config(**overrides: Any) -> JwtAuthConfig:
    """Build a valid HS256 config with targeted overrides."""
    params: dict[str, Any] = {"secret_path": _SECRET_PATH, "algorithms": ("HS256",)}
    params.update(overrides)
    return JwtAuthConfig(**params)


def _token(*, secret: str = _SECRET, exp_offset: int = 3600, **claims: Any) -> str:
    """Encode an HS256 token with a relative expiry and extra claims."""
    payload: dict[str, Any] = {"sub": "user-1", "exp": int(time.time()) + exp_offset}
    payload.update(claims)
    return pyjwt.encode(payload, secret, algorithm="HS256")


def _unsigned_token(**claims: Any) -> str:
    """Craft an ``alg: none`` token manually (pyjwt-independent)."""

    def _b64(part: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(part).encode()).rstrip(b"=").decode()

    payload: dict[str, Any] = {"sub": "user-1", "exp": int(time.time()) + 3600}
    payload.update(claims)
    return f"{_b64({'alg': 'none', 'typ': 'JWT'})}.{_b64(payload)}."


def _client(config: JwtAuthConfig) -> TestClient:
    """FastAPI app exposing the authenticated identity at GET /identity."""
    app = FastAPI()

    @app.get("/identity")
    async def read_identity() -> dict[str, Any]:
        identity = current_identity()
        return {
            "subject": identity.subject,
            "roles": list(identity.roles),
            "mechanism": identity.mechanism,
            "attributes": dict(identity.attributes),
        }

    app.add_middleware(JwtAuthMiddleware, config=config)
    return TestClient(app, raise_server_exceptions=False)


def _get(client: TestClient, token: str | None) -> Any:
    headers = {"Authorization": f"Bearer {token}"} if token is not None else {}
    return client.get("/identity", headers=headers)


def _hide_modules(monkeypatch: pytest.MonkeyPatch, prefix: str) -> None:
    """Simulate the absence of an optional dependency by nulling its modules."""
    for name in list(sys.modules):
        if name == prefix or name.startswith(f"{prefix}."):
            monkeypatch.setitem(sys.modules, name, None)
    monkeypatch.setitem(sys.modules, prefix, None)


# ---------------------------------------------------------------------------
# Config validation (fail-fast)
# ---------------------------------------------------------------------------


def test_config_parses_from_the_app_rest_auth_jwt_section() -> None:
    """The YAML section binds with the documented defaults (leeway 0, docs excluded)."""
    ctx = ConfigContext.from_dict(
        {"app": {"rest": {"auth": {"jwt": {"secret_path": _SECRET_PATH, "algorithms": ["HS256"]}}}}}
    )
    config = ctx.section("app.rest.auth.jwt", JwtAuthConfig)
    assert (config.leeway_seconds, "/openapi.json" in config.exclude_paths) == (0, True)


def test_config_fails_when_secret_and_public_keys_are_both_set() -> None:
    """Exactly one of ``secret_path``/``public_keys`` is allowed (fail-fast)."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(
            secret_path=_SECRET_PATH,
            public_keys={"k1": "-----BEGIN PUBLIC KEY-----"},
            algorithms=("HS256",),
        )


def test_config_fails_when_neither_secret_nor_public_keys_is_set() -> None:
    """A key source is mandatory: neither ``secret_path`` nor ``public_keys``."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(algorithms=("HS256",))


def test_config_fails_when_algorithms_is_empty() -> None:
    """An empty ``algorithms`` allowlist is rejected fail-fast."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(secret_path=_SECRET_PATH, algorithms=())


def test_config_forbids_the_none_algorithm_even_alongside_others() -> None:
    """``none`` is always forbidden in the allowlist, even mixed with HS256."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(secret_path=_SECRET_PATH, algorithms=("HS256", "none"))


def test_config_fails_when_hs_algorithm_is_paired_with_public_keys() -> None:
    """HS* requires ``secret``: pairing it with ``public_keys`` is incoherent."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(public_keys={"k1": "-----BEGIN PUBLIC KEY-----"}, algorithms=("HS256",))


def test_config_fails_when_rs_algorithm_is_paired_with_secret() -> None:
    """RS*/ES* require ``public_keys``: pairing them with ``secret`` is incoherent."""
    with pytest.raises(ConfigError):
        JwtAuthConfig(secret_path=_SECRET_PATH, algorithms=("RS256",))


# ---------------------------------------------------------------------------
# Middleware behaviour
# ---------------------------------------------------------------------------


def test_valid_token_passes_and_publishes_the_identity() -> None:
    """A valid HS256 token reaches the route as an identity, not as raw claims."""
    response = _get(_client(_config()), _token())
    assert (response.status_code, response.json()["subject"]) == (200, "user-1")


def test_expired_token_returns_401() -> None:
    """An expired ``exp`` claim is rejected with 401."""
    response = _get(_client(_config()), _token(exp_offset=-3600))
    assert response.status_code == 401


def test_invalid_signature_returns_401() -> None:
    """A token signed with a different secret is rejected with 401."""
    response = _get(_client(_config()), _token(secret="other-secret"))
    assert response.status_code == 401


def test_wrong_audience_returns_401() -> None:
    """When ``audience`` is configured, a mismatching ``aud`` claim is rejected."""
    response = _get(_client(_config(audience="loom-api")), _token(aud="other-api"))
    assert response.status_code == 401


def test_matching_audience_passes() -> None:
    """When ``audience`` is configured, a matching ``aud`` claim is accepted."""
    response = _get(_client(_config(audience="loom-api")), _token(aud="loom-api"))
    assert response.status_code == 200


def test_token_without_subject_returns_401() -> None:
    """A token with no ``sub`` carries no identity to bind or audit: rejected."""
    token = pyjwt.encode({"exp": int(time.time()) + 3600}, _SECRET, algorithm="HS256")
    response = _get(_client(_config()), token)
    assert response.status_code == 401


def test_missing_authorization_header_returns_401() -> None:
    """No ``Authorization`` header means no access (fail-closed)."""
    response = _get(_client(_config()), None)
    assert response.status_code == 401


@pytest.mark.parametrize(
    "header_value",
    ["Token abc", "Bearer", "bearer-xyz"],
    ids=["wrong-scheme", "bearer-without-token", "not-a-scheme"],
)
def test_malformed_authorization_header_returns_401(header_value: str) -> None:
    """A malformed ``Authorization`` header is rejected with 401."""
    response = _client(_config()).get("/claims", headers={"Authorization": header_value})
    assert response.status_code == 401


def test_alg_none_token_is_rejected() -> None:
    """An unsigned ``alg: none`` token is always rejected with 401."""
    response = _get(_client(_config()), _unsigned_token())
    assert response.status_code == 401


def test_default_exclude_paths_bypass_authentication() -> None:
    """``exclude_paths`` defaults keep /docs reachable without a token."""
    response = _client(_config()).get("/docs")
    assert response.status_code == 200


def test_401_body_uses_the_standard_error_shape() -> None:
    """The 401 body carries the framework error keys: code, message, trace_id."""
    response = _get(_client(_config()), None)
    assert {"code", "message", "trace_id"} <= set(response.json()["detail"])


def test_401_message_does_not_leak_the_cryptographic_reason() -> None:
    """Expired and bad-signature failures share one generic message (no oracle)."""
    client = _client(_config())
    expired = _get(client, _token(exp_offset=-3600)).json()["detail"]["message"]
    bad_signature = _get(client, _token(secret="other-secret")).json()["detail"]["message"]
    assert (expired == bad_signature, "signature" in expired.lower()) == (True, False)


def test_the_config_never_holds_the_shared_secret() -> None:
    """Redaction is not the guarantee: the value is never in the struct at all.

    A ``repr`` that hides the secret still leaves ``json.encode`` and
    ``to_builtins`` free to publish it, and those are what a config dump uses.
    Holding a path makes every serializer safe by construction.
    """
    config = _config()

    assert _SECRET not in repr(config)
    assert _SECRET.encode() not in msgspec.json.encode(config)
    assert _SECRET not in str(msgspec.to_builtins(config))
    assert config.verification_key(None) == _SECRET


def test_missing_pyjwt_extra_fails_at_startup_with_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Config present + pyjwt absent aborts startup with a 'loom-kernel[jwt]' hint."""
    config = _config()
    _hide_modules(monkeypatch, "jwt")
    with pytest.raises(ImportError, match=r"loom-kernel\[jwt\]"):
        JwtAuthMiddleware(_noop_asgi_app, config=config)


# ---------------------------------------------------------------------------
# Claims → Identity projection
# ---------------------------------------------------------------------------


async def _authenticate(config: JwtAuthConfig, token: str) -> Any:
    credentials = RequestCredentials(headers={"authorization": f"Bearer {token}"}, path="/")
    return await JwtAuthenticator(config).authenticate(credentials)


async def test_the_subject_claim_becomes_the_identity_subject() -> None:
    """``sub`` is the only claim the framework insists on: it is the caller."""
    identity = await _authenticate(_config(), _token(sub="user-7"))
    assert identity.subject == "user-7"


async def test_the_mechanism_is_recorded_on_the_identity() -> None:
    """The audit trail must state how the caller was authenticated."""
    identity = await _authenticate(_config(), _token())
    assert identity.mechanism == "jwt"


async def test_the_roles_claim_becomes_the_identity_roles() -> None:
    """Roles are read from the configured claim, never from a fixed name."""
    config = _config(roles_claim=_ROLES_CLAIM)
    identity = await _authenticate(config, _token(**{_ROLES_CLAIM: ["a", "b"]}))
    assert identity.roles == ("a", "b")


async def test_a_scalar_roles_claim_is_the_one_role_form() -> None:
    """A string claim is the single-role shape of the same contract."""
    config = _config(roles_claim=_ROLES_CLAIM)
    identity = await _authenticate(config, _token(**{_ROLES_CLAIM: "a"}))
    assert identity.roles == ("a",)


@pytest.mark.parametrize(
    "value",
    [123, True, {"role": "a"}, ["a", 7], [["a"]], None, []],
    ids=["int", "bool", "mapping", "mixed-list", "nested-list", "null", "empty"],
)
async def test_a_malformed_roles_claim_grants_no_role_at_all(value: Any) -> None:
    """A broken claim is not a partially authorized caller: it grants nothing."""
    config = _config(roles_claim=_ROLES_CLAIM)
    identity = await _authenticate(config, _token(**{_ROLES_CLAIM: value}))
    assert identity.roles == ()


async def test_no_configured_roles_claim_yields_no_roles() -> None:
    """Without a declared claim the mechanism binds no role, whatever the token says."""
    identity = await _authenticate(_config(), _token(**{_ROLES_CLAIM: ["a"]}))
    assert identity.roles == ()


async def test_string_custom_claims_become_identity_attributes() -> None:
    """Business claims travel as attributes so policies never parse a token."""
    identity = await _authenticate(_config(), _token(email=_EMAIL))
    assert identity.attribute("email") == _EMAIL


async def test_registered_claims_never_become_attributes() -> None:
    """``exp``/``iat``/``sub`` describe the token, not the caller."""
    identity = await _authenticate(_config(audience="loom-api"), _token(aud="loom-api"))
    assert set(identity.attributes) == set()


async def test_the_roles_claim_never_leaks_into_the_attributes() -> None:
    """Roles have their own field; duplicating them invites divergent checks."""
    config = _config(roles_claim=_ROLES_CLAIM)
    identity = await _authenticate(config, _token(**{_ROLES_CLAIM: "a"}))
    assert _ROLES_CLAIM not in identity.attributes


async def test_non_string_custom_claims_are_dropped() -> None:
    """Only string-valued claims cross into the identity attributes."""
    identity = await _authenticate(_config(), _token(seats=3, tags=["x"], email="a@b.com"))
    assert set(identity.attributes) == {"email"}


async def test_a_token_cannot_carry_a_reserved_loom_attribute() -> None:
    """A delegation is loom's to record: ``loom:`` claims never reach the identity."""
    claims = {"loom:on_behalf_of": "mallory", "attr_loom:on_behalf_of": "mallory", "email": _EMAIL}
    identity = await _authenticate(_config(), _token(**claims))
    assert set(identity.attributes) == {"email"}


async def test_provides_roles_reflects_the_configured_claim() -> None:
    """Startup gates rely on this flag to refuse role-based endpoints."""
    with_roles = JwtAuthenticator(_config(roles_claim=_ROLES_CLAIM)).provides_roles
    without_roles = JwtAuthenticator(_config()).provides_roles
    assert (with_roles, without_roles) == (True, False)


def test_an_unreadable_secret_path_fails_without_naming_the_path() -> None:
    """A missing secret file is a ConfigError whose message carries no filesystem detail.

    The path is the one thing an attacker probing config errors would want: it
    names where the signing material lives. ``from None`` also matters — an
    ``OSError`` in a chained traceback would print it anyway.
    """
    config = JwtAuthConfig(
        secret_path=str(pathlib.Path(tempfile.mkdtemp()) / "absent.key"),
        algorithms=("HS256",),
        roles_claim="roles",
    )

    with pytest.raises(ConfigError) as caught:
        config.verification_key(None)

    assert "absent.key" not in str(caught.value)
    assert caught.value.__cause__ is None
