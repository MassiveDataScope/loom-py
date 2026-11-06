"""Unit-of-work, invocation-scope and post-commit errors of the stores."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from loom.core.errors.base import SystemError
from loom.core.errors.codes import ErrorCode

if TYPE_CHECKING:
    from loom.core.persistence.stores import StoreKey
    from loom.core.uow.coordinator import InvocationOutcome


class ForeignSessionError(SystemError):
    """A caller met a session that another session manager or client opened.

    Each store, and each application, has its own session manager or client.
    A repository, a ``@transactional`` service and the elevation sink refuse
    to join a session their own manager did not open rather than write, or
    set elevation flags, through another engine with another store's
    row-level security settings. The invocation scope refuses a participant
    whose owner is not the caller's in the same way. Build it with
    :meth:`between`.

    Args:
        message: Names the caller's side and the active session's side.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.FOREIGN_SESSION)

    @staticmethod
    def side(store: str, connection: str) -> str:
        """Name one side of the error: a store and its connection.

        Args:
            store: The store's name.
            connection: The connection, without credentials, or what the side is.
        """
        return f"store {store!r} ({connection})"

    @classmethod
    def between(cls, own: str, active: str) -> ForeignSessionError:
        """The error for a caller on ``own`` meeting the active session of ``active``.

        Args:
            own: The store and connection of the caller, as :meth:`side` names them.
            active: The store and connection that opened the active session, likewise.
        """
        return cls(
            f"{own} cannot join the active session of {active}: "
            "one unit of work serves one session manager"
        )


class ScopeOwnershipError(SystemError):
    """A participant was touched from outside the task that owns the invocation scope.

    Raised from another task (a ``gather`` child, or a task that outlives its
    use case), from a thread, or once the scope has closed.

    Args:
        message: Which of those happened.
    """

    retryable = False
    """A programming error: a retry meets it again."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.INVOCATION_SCOPE_FOREIGN_TASK)


class UnitOfWorkInsideScope(SystemError):
    """``async with uow:`` was entered inside an invocation scope.

    The unit of work would open a second transaction that commits on its
    own, outside the invocation. Build it with :meth:`entering`.

    Args:
        message: Names the unit of work.
    """

    retryable = False
    """A programming error: a retry meets it again."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.UOW_INSIDE_SCOPE)

    @classmethod
    def entering(cls, unit_of_work: object) -> UnitOfWorkInsideScope:
        """The error for entering ``unit_of_work`` inside a scope.

        Args:
            unit_of_work: The unit of work being entered.
        """
        return cls(
            f"`async with {type(unit_of_work).__qualname__}` inside an invocation scope would "
            "commit on its own; the executor already runs the use case in its store's "
            "unit of work"
        )


class UndeclaredStoreWrite(SystemError):
    """A write targeted a store the running execution did not declare.

    An execution writes to its primary store, the ``__store__`` of its main
    model, and to the stores of its ``Joined[...]`` repositories; it reads
    from any store. The write is refused before the driver is called.

    Args:
        message: Names the store written to and the stores declared.
    """

    retryable = False
    """A programming error: a retry meets it again."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.UNDECLARED_STORE_WRITE)


class NestedIdentityMismatch(SystemError):
    """A nested execution joined its parent's unit of work as another identity.

    The shared transaction filters rows as one identity, so it cannot serve
    the child's as well; when it has not begun yet, the child's identity would
    be the one it begins with, for the parent too. Build it with :meth:`joining`.

    Args:
        message: Names the nested use case.
    """

    retryable = False
    """A programming error: a retry meets it again."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.NESTED_IDENTITY_MISMATCH)

    @classmethod
    def joining(cls, use_case: str) -> NestedIdentityMismatch:
        """The error for ``use_case`` joining the open invocation as another identity."""
        return cls(
            f"{use_case} would join the open unit of work as another identity; "
            "invoke it with the same identity, or dispatch a job that runs as the other one"
        )


class NestedElevation(SystemError):
    """A nested execution was started after its parent elevated a scope.

    The elevation flag is set on the shared transaction, and an elevation is
    never inherited. Build it with :meth:`opening`.

    Args:
        message: Names the nested use case and the elevated scopes.
    """

    retryable = False
    """A programming error: a retry meets it again."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.NESTED_ELEVATION)

    @classmethod
    def opening(cls, use_case: str, scopes: frozenset[str]) -> NestedElevation:
        """The error for starting ``use_case`` inside an execution holding ``scopes`` elevated."""
        return cls(
            f"{use_case} cannot run inside an execution that elevated "
            f"{', '.join(sorted(scopes))}; elevate inside {use_case} instead, or dispatch a job"
        )


class StoresNotCommitted(SystemError):
    """The stores of one invocation did not all end the same way.

    Some store committed, or wrote without a transaction, while another did
    not commit, or a store's commit result is unknown. loom never claims to
    have undone what committed: the outcome names every store for
    reconciliation. ``__cause__`` is the business error or the first failure.
    Build it with :meth:`of`.

    Args:
        message: Names every store and how it ended.
        outcome: The outcome of every store, in commit order.

    Attributes:
        outcome: The outcome of every store; ``None`` once the error crossed
            a process boundary: the causes it holds may carry driver details
            and may not pickle, so only the message travels.
    """

    retryable = False
    """A store may have committed: a retry would apply its write twice."""

    def __init__(self, message: str, outcome: InvocationOutcome | None = None) -> None:
        self.outcome = outcome
        super().__init__(message, code=ErrorCode.STORES_NOT_COMMITTED)

    @classmethod
    def of(cls, outcome: InvocationOutcome) -> StoresNotCommitted:
        """The error for ``outcome``, whose message names every store and how it ended.

        Args:
            outcome: The outcome of every store, in commit order.
        """
        stores = ", ".join(f"{store.key}={type(store).__name__}" for store in outcome.stores)
        return cls(f"the stores of this invocation did not all commit: {stores}", outcome)

    def __reduce__(self) -> tuple[type[StoresNotCommitted], tuple[str]]:
        return type(self), (self.message,)


class UncomposedStore(SystemError):
    """A use case enlisted a store that has no unit of work in this application.

    The store is not declared, or its backend persists nothing.

    Args:
        key: The store enlisted.
        composed: The stores that have a unit of work.
    """

    retryable = False
    """A configuration error: a retry meets it again."""

    def __init__(self, key: StoreKey, composed: Iterable[StoreKey]) -> None:
        super().__init__(
            f"store {key!r} has no unit of work in this application; "
            f"the stores with one are {sorted(composed)}",
            code=ErrorCode.UNCOMPOSED_STORE,
        )


class PostCommitError(SystemError):
    """Raised when one or more post-commit actions failed after a commit.

    Every action runs even when an earlier one fails; the error carries all
    failures in priority, publish, then plain order.

    Args:
        committed: Whether the transaction had committed when the actions ran.
        failures: Exceptions in priority, publish, then plain order.
    """

    def __init__(self, *, committed: bool, failures: tuple[Exception, ...]) -> None:
        self.committed = committed
        # A retry once the transaction committed would apply it twice.
        self.retryable = not committed
        self.failures = failures
        super().__init__(
            f"{len(failures)} post-commit action(s) failed (committed={committed})",
            code=ErrorCode.POST_COMMIT_FAILURE,
        )
