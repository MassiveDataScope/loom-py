"""The authenticated caller of one execution, as a domain value object.

``Identity`` is deliberately transport-agnostic: it says *who* is calling and
*what they hold*, never *how* they proved it beyond a free-form ``mechanism``
label.  A JWT authenticator, a mutual-TLS one, or a signed-header one all
produce the same shape, so business policies never learn what a token is.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from loom.core.errors import Forbidden, Unauthenticated

_EMPTY_ATTRIBUTES: Mapping[str, str] = MappingProxyType({})
RESERVED_ATTRIBUTE_PREFIX = "loom:"
"""Attributes under this prefix are set only by loom itself, never by an authenticator."""
ON_BEHALF_OF_ATTRIBUTE = f"{RESERVED_ATTRIBUTE_PREFIX}on_behalf_of"
"""The person a system identity acts for; set by loom's event dispatchers and audited."""

_NO_SUBJECT_MESSAGE = "Authentication required: the request carries no verified identity."


@dataclass(frozen=True, slots=True)
class Identity:
    """Verified caller of a single execution.

    Instances are immutable and safe to share across an async context.  Only
    the transport layer builds them, from credentials it has already verified;
    every other layer consumes them read-only.

    Attributes:
        subject: Stable identifier of the caller.  Empty means anonymous.
        roles: Roles the caller holds, in the order the mechanism reported
            them and without duplicates.
        attributes: Verified string-valued facts about the caller (e-mail,
            tenant, department, ...).  Copied on construction, so a later
            mutation of the source mapping cannot rewrite the identity.
        mechanism: Label of the mechanism that authenticated the caller (e.g.
            ``"jwt"``).  Used for the audit trail, never for authorization.

    Example::

        identity = Identity(
            subject="user-1",
            roles=("role_viz_reader",),
            attributes={"email": "ada@example.com"},
            mechanism="jwt",
        )
        if identity.has_role("role_viz_reader"):
            ...
    """

    subject: str
    roles: tuple[str, ...] = ()
    # default_factory, not default: on Python 3.11 dataclasses reject an
    # unhashable default, and a mappingproxy is unhashable. The factory hands
    # back the same frozen empty mapping, so nothing is allocated per instance.
    attributes: Mapping[str, str] = field(default_factory=lambda: _EMPTY_ATTRIBUTES)
    mechanism: str = ""

    def __post_init__(self) -> None:
        """Freeze the attribute mapping so the identity cannot be rewritten.

        The copy is the point: without it, whoever built the identity keeps a
        live handle on the data an authorization decision is taken from.
        """
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))

    @property
    def is_authenticated(self) -> bool:
        """Whether the caller was identified by an authentication mechanism.

        Returns:
            ``True`` when a non-empty subject is present.
        """
        return bool(self.subject)

    def has_role(self, role: str) -> bool:
        """Report whether the caller holds *role*.

        Matching is exact: no case folding and no prefix matching, so a role
        name can never be widened by accident.

        Args:
            role: Role name to look for.

        Returns:
            ``True`` when the caller holds exactly that role.
        """
        return role in self.roles

    def attribute(self, name: str) -> str | None:
        """Return the verified attribute *name*, or ``None`` when absent.

        Args:
            name: Attribute key as published by the authenticator.

        Returns:
            The attribute value, or ``None`` when the caller does not carry it.
        """
        return self.attributes.get(name)

    def require_subject(self) -> str:
        """Return the subject, refusing anonymous callers.

        Returns:
            The caller subject.

        Raises:
            Unauthenticated: When the identity is anonymous.
        """
        if not self.subject:
            raise Unauthenticated(_NO_SUBJECT_MESSAGE)
        return self.subject

    def require_attribute(self, name: str) -> str:
        """Return a mandatory verified attribute.

        The two failure modes are distinct on purpose: an anonymous caller can
        fix the request by authenticating (401), while an authenticated caller
        missing the attribute cannot (403).

        Args:
            name: Attribute key the caller must carry.

        Returns:
            The attribute value.

        Raises:
            Unauthenticated: When the identity is anonymous.
            Forbidden: When the authenticated caller does not carry *name*.
        """
        self.require_subject()
        value = self.attributes.get(name)
        if value is None:
            raise Forbidden(f"The caller identity carries no {name!r} attribute.")
        return value

    def __repr__(self) -> str:
        """Render the identity without ever echoing an attribute value.

        Attribute *names* are needed to debug a policy reading the wrong key;
        their values are personal data and must not reach a log through a
        stray repr.
        """
        names = ", ".join(sorted(self.attributes))
        return (
            f"Identity(subject={self.subject!r}, roles={self.roles!r}, "
            f"mechanism={self.mechanism!r}, attributes=[{names}])"
        )


ANONYMOUS = Identity(subject="")
"""The absence of an identity, as an explicit value instead of ``None``."""
