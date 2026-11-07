"""The provider writes who acts and under which trace into every transaction."""

from __future__ import annotations

import pytest

from loom.core.identity import ON_BEHALF_OF_ATTRIBUTE, Identity
from loom.core.identity.context import reset_identity, set_identity
from loom.core.repository.sqlalchemy.rls.provider import (
    ACTOR_MECHANISM,
    ACTOR_ON_BEHALF_OF,
    ACTOR_SUBJECT,

    TRACE_ID,
    scoped_session_settings,
)
from loom.core.tracing.context import active_trace_id

_ACTOR_KEYS = (ACTOR_SUBJECT, ACTOR_MECHANISM, ACTOR_ON_BEHALF_OF, TRACE_ID)


def _provider(product=None):
    return scoped_session_settings({}, {}, product, path="stores.default")


def test_an_anonymous_transaction_writes_every_actor_key_empty() -> None:
    settings = _provider()()

    assert {key: settings[key] for key in _ACTOR_KEYS} == dict.fromkeys(_ACTOR_KEYS, "")


def test_the_identity_and_trace_reach_the_transaction() -> None:
    token = set_identity(Identity(subject="ana", mechanism="jwt"))
    try:
        with active_trace_id("trace-1"):
            settings = _provider()()
    finally:
        reset_identity(token)

    assert settings[ACTOR_SUBJECT] == "ana"
    assert settings[ACTOR_MECHANISM] == "jwt"
    assert settings[ACTOR_ON_BEHALF_OF] == ""
    assert settings[TRACE_ID] == "trace-1"


def test_delegation_comes_only_from_the_reserved_attribute() -> None:
    identity = Identity(
        subject="svc:invoices",
        mechanism="lambda:f:s3",
        attributes={ON_BEHALF_OF_ATTRIBUTE: "ana", "on_behalf_of": "mallory"},
    )
    token = set_identity(identity)
    try:
        settings = _provider()()
    finally:
        reset_identity(token)

    assert settings[ACTOR_ON_BEHALF_OF] == "ana"


def test_a_product_key_under_the_loom_namespace_is_refused() -> None:
    provider = _provider(lambda: {"loom.actor.subject": "forged"})

    with pytest.raises(ValueError, match="loom."):
        provider()


def test_product_keys_outside_the_loom_namespace_are_kept() -> None:
    settings = _provider(lambda: {"app.tenant": "t1"})()

    assert settings["app.tenant"] == "t1"
