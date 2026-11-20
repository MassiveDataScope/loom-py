"""The errors loom raises at run time, by area; every class is importable from here."""

from loom.core.errors.auth import (
    DelegationExpired,
    Forbidden,
    IdentityFixed,
    InsufficientAuthentication,
    MissingCaller,
    Unauthenticated,
)
from loom.core.errors.base import DomainError, LoomError, SystemError
from loom.core.errors.container import ContainerSealed, ResolutionError
from loom.core.errors.domain import (
    BODY_FIELD,
    Conflict,
    NotFound,
    RateLimited,
    RuleViolation,
    RuleViolations,
    UnsupportedQuery,
)
from loom.core.errors.jobs import (
    JobFailedError,
    JobTimeoutError,
    MalformedJobMessage,
    UnknownJob,
)
from loom.core.errors.model import BoundaryValidationError, UnsupportedBoundaryType
from loom.core.errors.stores import (
    ForeignSessionError,
    NestedElevation,
    NestedIdentityMismatch,
    PostCommitError,
    ScopeOwnershipError,
    StoresNotCommitted,
    UncomposedStore,
    UndeclaredStoreWrite,
    UnitOfWorkInsideScope,
)

__all__ = [
    "BODY_FIELD",
    "BoundaryValidationError",
    "Conflict",
    "ContainerSealed",
    "DelegationExpired",
    "DomainError",
    "Forbidden",
    "IdentityFixed",
    "ForeignSessionError",
    "InsufficientAuthentication",
    "JobFailedError",
    "JobTimeoutError",
    "LoomError",
    "MalformedJobMessage",
    "MissingCaller",
    "NestedElevation",
    "NestedIdentityMismatch",
    "NotFound",
    "ResolutionError",
    "PostCommitError",
    "RateLimited",
    "RuleViolation",
    "RuleViolations",
    "ScopeOwnershipError",
    "StoresNotCommitted",
    "SystemError",
    "Unauthenticated",
    "UncomposedStore",
    "UndeclaredStoreWrite",
    "UnitOfWorkInsideScope",
    "UnknownJob",
    "UnsupportedBoundaryType",
    "UnsupportedQuery",
]
