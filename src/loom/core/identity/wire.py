"""Serialisation of the caller identity for job envelopes.

A job crosses a process boundary, where no context variable follows it.  The
identity therefore travels inside the envelope, as an explicit part of the wire
contract, encoded with plain JSON types so any broker serializer can carry it.

The deadline of the caller's delegation travels inside it, under ``until``
(:class:`Delegation`): a deadline never travels without a caller, and
the worker refuses a job whose deadline has passed.

Decoding treats its input as untrusted: anything that cannot be read as an
identity becomes ``None``, and a use case declaring ``Caller()`` then fails
closed instead of running as a partially decoded caller.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any, NamedTuple

from loom.core.identity.identity import Identity

_SUBJECT = "subject"
_ROLES = "roles"
_ATTRIBUTES = "attributes"
_MECHANISM = "mechanism"
_UNTIL = "until"


def encode_identity(identity: Identity | None) -> dict[str, Any] | None:
    """Encode *identity* for transport inside a job envelope.

    Args:
        identity: Caller to propagate, or ``None``.

    Returns:
        A JSON-serialisable mapping, or ``None`` when there is no
        authenticated caller to propagate.

    Example::

        envelope = {"payload": payload, "identity": encode_identity(caller)}
    """
    return Delegation(identity, None).encoded()


def decode_identity(payload: Any) -> Identity | None:
    """Rebuild the caller from a job envelope, refusing anything unreadable.

    Args:
        payload: Value found under the envelope's identity key.

    Returns:
        The decoded identity, or ``None`` when the envelope carries none or
        carries something that is not one.
    """
    return Delegation.decoded(payload).caller


class Delegation(NamedTuple):
    """A caller and the deadline of its delegation, as dispatched work carries them.

    Attributes:
        caller: The caller, or ``None`` for no caller.
        until: The deadline of its delegation (a Unix time), or ``None`` for a
            caller whose worker starts one.
    """

    caller: Identity | None
    until: float | None

    @classmethod
    def of(cls, caller: Identity | None, until: float | None) -> Delegation:
        """Return *caller* with *until*, or :data:`NO_DELEGATION` for no authenticated caller.

        Args:
            caller: The caller; ``None`` or an unauthenticated one carries none.
            until: The deadline of its delegation, or ``None`` for one its
                worker starts.

        Returns:
            The delegation dispatched work carries.
        """
        if caller is None or not caller.is_authenticated:
            return NO_DELEGATION
        return cls(caller, until)

    def encoded(self) -> dict[str, Any] | None:
        """Encode the caller, with ``until`` when it has a deadline, for a job envelope.

        Returns:
            A JSON-serialisable mapping, or ``None`` when there is no
            authenticated caller. A non-finite deadline is encoded as
            :data:`EXPIRED`.
        """
        caller, until = self
        if caller is None or not caller.is_authenticated:
            return None
        encoded: dict[str, Any] = {
            _SUBJECT: caller.subject,
            _ROLES: list(caller.roles),
            _ATTRIBUTES: dict(caller.attributes),
            _MECHANISM: caller.mechanism,
        }
        if until is not None:
            encoded[_UNTIL] = until if math.isfinite(until) else EXPIRED
        return encoded

    @classmethod
    def decoded(cls, payload: object) -> Delegation:
        """Rebuild the caller and its deadline from a job envelope, failing closed.

        The payload is untrusted: anything that cannot be read as a caller
        decodes to no caller, and only string roles and string-valued
        attributes are kept.

        Args:
            payload: Value found under the envelope's identity key.

        Returns:
            No caller and no deadline when no caller can be read. Otherwise the caller
            with: no deadline when it carries no ``until``; the deadline when
            it is a finite number; and :data:`EXPIRED` for anything else
            (``inf``, ``nan``, ``null``, a string, an int too large for a
            float).
        """
        if not isinstance(payload, Mapping):
            return NO_DELEGATION
        subject = payload.get(_SUBJECT)
        if not isinstance(subject, str) or not subject:
            return NO_DELEGATION
        mechanism = payload.get(_MECHANISM)
        caller = Identity(
            subject=subject,
            roles=cls._roles(payload.get(_ROLES)),
            attributes=cls._attributes(payload.get(_ATTRIBUTES)),
            mechanism=mechanism if isinstance(mechanism, str) else "",
        )
        return cls(caller, cls._deadline(payload[_UNTIL]) if _UNTIL in payload else None)

    @staticmethod
    def _roles(value: object) -> tuple[str, ...]:
        """Keep the non-empty string roles, dropping anything else."""
        if not isinstance(value, (list, tuple)):
            return ()
        return tuple(item for item in value if isinstance(item, str) and item)

    @staticmethod
    def _attributes(value: object) -> dict[str, str]:
        """Keep the string-valued attributes, dropping anything else."""
        if not isinstance(value, Mapping):
            return {}
        return {
            name: item
            for name, item in value.items()
            if isinstance(name, str) and isinstance(item, str)
        }

    @staticmethod
    def _deadline(value: object) -> float:
        """Read a finite deadline, or :data:`EXPIRED` for anything else."""
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return EXPIRED
        try:
            deadline = float(value)
        except OverflowError:
            return EXPIRED
        return deadline if math.isfinite(deadline) else EXPIRED


NO_DELEGATION = Delegation(None, None)
"""No caller and no deadline: what work dispatched with no authenticated caller carries."""

EXPIRED = 0.0
"""The deadline an unreadable one decodes to: the epoch, long passed."""
