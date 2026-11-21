"""The identity wire contract: what survives a broker hop, and what is refused.

A job envelope crosses a process boundary, so the codec treats its input as
untrusted: anything it cannot read as an identity decodes to ``None``, and a
use case declaring ``Caller()`` then fails closed instead of running as a
half-decoded caller.
"""

from __future__ import annotations

import math
from typing import Any

import pytest

from loom.core.identity import ANONYMOUS, NO_DELEGATION, Delegation, Identity
from loom.core.identity.wire import EXPIRED, decode_identity, encode_identity

_SUBJECT = "user-1"
_ROLES = ("role_a", "role_b")
_ATTRIBUTES = {"email": "ada@example.com"}
_MECHANISM = "jwt"


def _identity() -> Identity:
    return Identity(
        subject=_SUBJECT,
        roles=_ROLES,
        attributes=_ATTRIBUTES,
        mechanism=_MECHANISM,
    )


def _roundtrip(identity: Identity) -> Identity | None:
    return decode_identity(encode_identity(identity))


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


class TestIdentityOnTheWire:
    def test_the_identity_survives_the_round_trip(self) -> None:
        """Every field the authorization rules read must cross the wire intact."""
        assert _roundtrip(_identity()) == _identity()

    def test_the_encoded_form_is_plain_json_types(self) -> None:
        """The envelope travels through a JSON broker serializer, not pickle."""
        encoded = encode_identity(_identity())
        assert encoded == {
            "subject": _SUBJECT,
            "roles": list(_ROLES),
            "attributes": dict(_ATTRIBUTES),
            "mechanism": _MECHANISM,
        }

    def test_an_anonymous_identity_encodes_to_none(self) -> None:
        """Anonymity is the absence of a caller: nothing to put on the wire."""
        assert encode_identity(Identity(subject="")) is None

    def test_no_identity_encodes_to_none(self) -> None:
        """A dispatch without a caller must not invent an envelope field."""
        assert encode_identity(None) is None

    # ---------------------------------------------------------------------------
    # Decoding untrusted input
    # ---------------------------------------------------------------------------

    def test_an_absent_envelope_decodes_to_no_identity(self) -> None:
        """An envelope predating the contract carries no identity, and says so."""
        assert decode_identity(None) is None

    @pytest.mark.parametrize(
        "payload",
        [
            {},
            {"subject": ""},
            {"subject": 7},
            {"roles": ["role_a"]},
            "not-a-mapping",
            [],
        ],
        ids=["empty", "blank-subject", "non-string-subject", "no-subject", "string", "list"],
    )
    def test_an_unreadable_envelope_decodes_to_no_identity(self, payload: Any) -> None:
        """Fail-closed: a half-understood envelope is not a caller."""
        assert decode_identity(payload) is None

    def test_non_string_roles_are_dropped(self) -> None:
        """A tampered envelope cannot smuggle a non-string role into an allowlist check."""
        identity = decode_identity({"subject": _SUBJECT, "roles": ["role_a", 7, None]})
        assert identity is not None
        assert identity.roles == ("role_a",)

    def test_non_string_attributes_are_dropped(self) -> None:
        """Attributes are string-valued by contract, on the wire as much as in memory."""
        identity = decode_identity({"subject": _SUBJECT, "attributes": {"a": "1", "b": 2}})
        assert identity is not None
        assert dict(identity.attributes) == {"a": "1"}

    def test_a_missing_mechanism_decodes_to_an_empty_label(self) -> None:
        """The mechanism is an audit label, not an authorization input: never mandatory."""
        identity = decode_identity({"subject": _SUBJECT})
        assert identity is not None
        assert identity.mechanism == ""


class TestDelegation:
    """The deadline travels inside the identity, under ``until``."""

    def test_a_finite_deadline_survives_the_round_trip(self) -> None:
        encoded = Delegation(_identity(), 1234.5).encoded()

        assert Delegation.decoded(encoded) == Delegation(_identity(), 1234.5)

    def test_a_caller_without_one_decodes_with_none_for_the_worker_to_start(self) -> None:
        assert Delegation.decoded(encode_identity(_identity())) == Delegation(_identity(), None)

    @pytest.mark.parametrize("payload", [None, {}, "acme"], ids=["absent", "empty", "string"])
    def test_no_readable_caller_decodes_to_no_caller_and_no_deadline(self, payload: Any) -> None:
        assert Delegation.decoded(payload) is NO_DELEGATION

    @pytest.mark.parametrize("caller", [None, ANONYMOUS], ids=["no-caller", "anonymous"])
    def test_from_caller_no_authenticated_caller_is_no_delegation(
        self, caller: Identity | None
    ) -> None:
        assert Delegation.from_caller(caller, 1234.5) is NO_DELEGATION

    def test_from_caller_an_authenticated_caller_keeps_its_deadline(self) -> None:
        assert Delegation.from_caller(_identity(), 1234.5) == Delegation(_identity(), 1234.5)

    @pytest.mark.parametrize(
        "until",
        [math.inf, math.nan, "soon", True, None, 10**400],
        ids=["infinite", "nan", "string", "bool", "null", "huge-int"],
    )
    def test_an_unreadable_deadline_has_expired(self, until: Any) -> None:
        """``null`` included: msgspec writes ``inf`` as ``null``, never a fresh window."""
        payload = {**(encode_identity(_identity()) or {}), "until": until}

        assert Delegation.decoded(payload) == Delegation(_identity(), EXPIRED)

    @pytest.mark.parametrize("until", [math.inf, math.nan], ids=["infinite", "nan"])
    def test_a_non_finite_deadline_is_encoded_as_expired(self, until: float) -> None:
        assert (Delegation(_identity(), until).encoded() or {})["until"] == EXPIRED

    @pytest.mark.parametrize("caller", [None, Identity(subject="")], ids=["none", "anonymous"])
    def test_no_authenticated_caller_encodes_to_none(self, caller: Identity | None) -> None:
        assert Delegation(caller, 1234.5).encoded() is None
