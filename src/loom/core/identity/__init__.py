"""Caller identity: the verified answer to "who is running this?".

Exposes the immutable :class:`Identity` value object, the explicit
:data:`ANONYMOUS` absence of one, and the context guard that propagates it
across the async stack.  This package belongs to the domain layer: it imports
no transport and no infrastructure.
"""

from loom.core.identity.context import current_identity, reset_identity, set_identity
from loom.core.identity.identity import (
    ANONYMOUS,
    ON_BEHALF_OF_ATTRIBUTE,
    RESERVED_ATTRIBUTE_PREFIX,
    Identity,
)
from loom.core.identity.issuer import IssuedToken, TokenIssuer

__all__ = [
    "ANONYMOUS",
    "ON_BEHALF_OF_ATTRIBUTE",
    "RESERVED_ATTRIBUTE_PREFIX",
    "Identity",
    "IssuedToken",
    "TokenIssuer",
    "current_identity",
    "reset_identity",
    "set_identity",
]
