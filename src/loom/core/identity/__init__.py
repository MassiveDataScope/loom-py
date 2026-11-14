"""Caller identity: the verified answer to "who is running this?".

Exposes the immutable :class:`Identity` value object, the explicit
:data:`ANONYMOUS` absence of one, and the one carrier of the acting identity,
which everything a request causes inherits.  This package belongs to the
domain layer: it imports no transport and no infrastructure.
"""

from loom.core.errors import IdentityFixed
from loom.core.identity.context import (
    DEFAULT_MAX_DELEGATION_SECONDS,
    Acting,
    configure_delegation,
    current_caller,
    current_identity,
    delegation,
    fix_identity,
    refuse_expired_delegation,
    reset_identity,
    set_identity,
)
from loom.core.identity.identity import (
    ANONYMOUS,
    ON_BEHALF_OF_ATTRIBUTE,
    RESERVED_ATTRIBUTE_PREFIX,
    Identity,
)
from loom.core.identity.issuer import IssuedToken, TokenIssuer

__all__ = [
    "ANONYMOUS",
    "DEFAULT_MAX_DELEGATION_SECONDS",
    "Acting",
    "ON_BEHALF_OF_ATTRIBUTE",
    "RESERVED_ATTRIBUTE_PREFIX",
    "Identity",
    "IdentityFixed",
    "IssuedToken",
    "TokenIssuer",
    "configure_delegation",
    "current_caller",
    "current_identity",
    "delegation",
    "fix_identity",
    "refuse_expired_delegation",
    "reset_identity",
    "set_identity",
]
