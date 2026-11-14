"""The ``Identity`` value object: what it guarantees and what it never leaks."""

from __future__ import annotations

import pytest

from loom.core.errors import Forbidden, Unauthenticated
from loom.core.identity import ANONYMOUS, Identity

_EMAIL = "ada@example.com"
_SUBJECT = "user-1"
_ROLE = "role_viz_reader"


def _identity(**overrides: object) -> Identity:
    params: dict[str, object] = {
        "subject": _SUBJECT,
        "roles": (_ROLE,),
        "attributes": {"email": _EMAIL},
        "mechanism": "jwt",
    }
    params.update(overrides)
    return Identity(**params)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Authentication state
# ---------------------------------------------------------------------------


class TestIdentityValue:
    def test_an_identity_with_a_subject_is_authenticated(self) -> None:
        """A non-empty subject is what makes an identity authenticated."""
        assert _identity().is_authenticated is True

    def test_the_anonymous_identity_is_not_authenticated(self) -> None:
        """``ANONYMOUS`` is the explicit 'nobody', never a half-authenticated caller."""
        assert (ANONYMOUS.is_authenticated, ANONYMOUS.roles) == (False, ())

    def test_an_empty_subject_is_never_authenticated(self) -> None:
        """A blank subject carries nothing to bind or audit."""
        assert Identity(subject="").is_authenticated is False

    # ---------------------------------------------------------------------------
    # Roles and attributes
    # ---------------------------------------------------------------------------

    def test_has_role_is_true_for_a_held_role(self) -> None:
        """Role membership is an exact match, never a prefix or case-insensitive one."""
        assert _identity().has_role(_ROLE) is True

    @pytest.mark.parametrize(
        "candidate",
        ["role_admin", _ROLE.upper(), _ROLE[:5], ""],
        ids=["other-role", "different-case", "prefix", "empty"],
    )
    def test_has_role_is_false_for_anything_but_an_exact_match(self, candidate: str) -> None:
        """Anything but the exact role name is refused (fail-closed)."""
        assert _identity().has_role(candidate) is False

    def test_attribute_returns_the_verified_value(self) -> None:
        """Attributes expose the verified, string-typed claims of the caller."""
        assert _identity().attribute("email") == _EMAIL

    def test_attribute_returns_none_when_absent(self) -> None:
        """An absent attribute reads as ``None``, never as an empty string."""
        assert _identity().attribute("department") is None

    def test_attributes_are_immutable(self) -> None:
        """The attribute mapping cannot be mutated after construction."""
        attributes = _identity().attributes
        with pytest.raises(TypeError):
            attributes["email"] = "evil@example.com"  # type: ignore[index]

    def test_mutating_the_source_mapping_does_not_change_the_identity(self) -> None:
        """The identity copies its attributes: the caller cannot rewrite them later."""
        source = {"email": _EMAIL}
        identity = Identity(subject=_SUBJECT, attributes=source)
        source["email"] = "evil@example.com"
        assert identity.attribute("email") == _EMAIL

    # ---------------------------------------------------------------------------
    # require_* — fail-closed accessors
    # ---------------------------------------------------------------------------

    def test_require_subject_returns_the_subject_when_authenticated(self) -> None:
        """The happy path returns the subject unchanged."""
        assert _identity().require_subject() == _SUBJECT

    def test_require_subject_raises_unauthenticated_when_anonymous(self) -> None:
        """No identity is a 401 condition, not a 403 one."""
        identity = ANONYMOUS
        with pytest.raises(Unauthenticated):
            identity.require_subject()

    def test_require_attribute_returns_the_value_when_present(self) -> None:
        """The happy path returns the verified attribute value."""
        assert _identity().require_attribute("email") == _EMAIL

    def test_require_attribute_raises_unauthenticated_when_anonymous(self) -> None:
        """Without an identity the caller must authenticate first (401)."""
        identity = ANONYMOUS
        with pytest.raises(Unauthenticated):
            identity.require_attribute("email")

    def test_require_attribute_raises_forbidden_when_the_attribute_is_missing(self) -> None:
        """An authenticated caller lacking the attribute is refused (403), not challenged."""
        identity = _identity()
        with pytest.raises(Forbidden):
            identity.require_attribute("department")

    def test_require_attribute_error_names_the_attribute_without_its_value(self) -> None:
        """The message must be actionable but must not echo any attribute value."""
        identity = _identity()
        with pytest.raises(Forbidden, match="department"):
            identity.require_attribute("department")

    # ---------------------------------------------------------------------------
    # repr — attribute names are debuggable, values are PII
    # ---------------------------------------------------------------------------

    def test_repr_exposes_the_attribute_names(self) -> None:
        """Names are needed to debug a policy that reads the wrong attribute."""
        assert "email" in repr(_identity())

    def test_repr_never_exposes_an_attribute_value(self) -> None:
        """Values are PII: they must not reach logs through a stray repr."""
        assert _EMAIL not in repr(_identity())

    def test_repr_exposes_the_roles_and_the_mechanism(self) -> None:
        """Roles and mechanism are the audit trail of an authorization decision."""
        rendered = repr(_identity())
        assert (_ROLE in rendered, "jwt" in rendered) == (True, True)
