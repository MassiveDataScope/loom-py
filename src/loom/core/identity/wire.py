"""Serialisation of the caller identity for job envelopes.

A job crosses a process boundary, where no context variable follows it.  The
identity therefore travels inside the envelope, as an explicit part of the wire
contract, encoded with plain JSON types so any broker serializer can carry it.

The deadline of the caller's delegation travels inside it, under ``until``
(:func:`decode_delegation`): a deadline never travels without a caller, and
the worker refuses a job whose deadline has passed.

Decoding treats its input as untrusted: anything that cannot be read as an
identity becomes ``None``, and a use case declaring ``Caller()`` then fails
closed instead of running as a partially decoded caller.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from loom.core.identity.identity import Identity

_SUBJECT = "subject"
_ROLES = "roles"
_ATTRIBUTES = "attributes"
_MECHANISM = "mechanism"
_UNTIL = "until"


def encode_identity(identity: Identity | None, until: float | None = None) -> dict[str, Any] | None:
    """Encode *identity*, and the deadline of its delegation, for transport inside a job envelope.

    Args:
        identity: Caller to propagate, or ``None``.
        until: The deadline of the caller's delegation (a Unix time); none
            for a caller whose worker starts one (a system identity).

    Returns:
        A JSON-serialisable mapping, or ``None`` when there is no
        authenticated caller to propagate.

    Example::

        envelope = {"payload": payload, "identity": encode_identity(caller)}
    """
    if identity is None or not identity.is_authenticated:
        return None
    encoded: dict[str, Any] = {
        _SUBJECT: identity.subject,
        _ROLES: list(identity.roles),
        _ATTRIBUTES: dict(identity.attributes),
        _MECHANISM: identity.mechanism,
    }
    if until is not None:
        encoded[_UNTIL] = until if math.isfinite(until) else EXPIRED
    return encoded


def decode_identity(payload: Any) -> Identity | None:
    """Rebuild the caller from a job envelope, refusing anything unreadable.

    Args:
        payload: Value found under the envelope's identity key.  Any type is
            accepted because the envelope may predate this contract or come
            from a tampered broker message.

    Returns:
        The decoded identity, or ``None`` when the envelope carries none or
        carries something that is not one.
    """
    if not isinstance(payload, Mapping):
        return None
    subject = payload.get(_SUBJECT)
    if not isinstance(subject, str) or not subject:
        return None
    mechanism = payload.get(_MECHANISM)
    return Identity(
        subject=subject,
        roles=_decode_roles(payload.get(_ROLES)),
        attributes=_decode_attributes(payload.get(_ATTRIBUTES)),
        mechanism=mechanism if isinstance(mechanism, str) else "",
    )


def decode_delegation(payload: Any) -> tuple[Identity | None, float | None]:
    """Rebuild the caller and the deadline of its delegation from a job envelope, failing closed.

    Args:
        payload: Value found under the envelope's identity key.

    Returns:
        ``(None, None)`` when no caller can be read. Otherwise the caller
        with: ``None`` when it carries no ``until`` (an earlier version's
        message, a system identity), so the worker starts a delegation; the
        deadline when it is a finite number, which
        :func:`~loom.core.identity.set_identity` clamps to one window; and
        :data:`EXPIRED` for anything else (``inf``, ``nan``, ``null``, a
        string, an int too large for a float), so the job is refused.
    """
    caller = decode_identity(payload)
    if caller is None:
        return None, None
    if _UNTIL not in payload:
        return caller, None
    until = payload[_UNTIL]
    if isinstance(until, bool) or not isinstance(until, (int, float)):
        return caller, EXPIRED
    try:
        deadline = float(until)
    except OverflowError:
        return caller, EXPIRED
    return caller, deadline if math.isfinite(deadline) else EXPIRED


EXPIRED = 0.0
"""The deadline an unreadable one decodes to: the epoch, long passed."""


def _decode_roles(value: Any) -> tuple[str, ...]:
    """Keep the non-empty string roles, dropping anything else."""
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(item for item in value if isinstance(item, str) and item)


def _decode_attributes(value: Any) -> dict[str, str]:
    """Keep the string-valued attributes, dropping anything else."""
    if not isinstance(value, Mapping):
        return {}
    return {
        name: item
        for name, item in value.items()
        if isinstance(name, str) and isinstance(item, str)
    }
