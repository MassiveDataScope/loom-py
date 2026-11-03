"""Backend-neutral signal for "is an atomic transaction open right now".

A collaborator that defers work until after a commit (the cache bump is the
first one) needs to answer exactly one question: will the current write be
followed by a commit later, or is it already durable? A richer scope object
— one that also named the owner, or nested depth, or carried mutation
events — would duplicate state two other mechanisms already own:
:mod:`loom.core.engine.post_commit` decides *where* a deferred action runs,
and the ``_mutations`` ``ContextVar`` in
:mod:`loom.core.repository.sqlalchemy.transactional` decides *what* the
``@transactional`` decorator's own hooks receive. Both would need to change
in lockstep with a scope object here; a bare boolean does not.

The boolean is set by the two units of work that own a transaction that will
later commit: :class:`~loom.core.repository.sqlalchemy.uow.SQLAlchemyUnitOfWork`
(which an outermost
:func:`~loom.core.repository.sqlalchemy.transactional.transactional` also
runs in) and :class:`~loom.core.repository.mongo.uow.MongoUnitOfWork`. A
backend without a transaction (a no-op unit of work, DynamoDB's autocommit)
never sets it, so a caller elsewhere in the stack still sees ``False`` and
keeps its current, already-correct behaviour.

Every opener resets this token before its other resets, in the same
``finally``: if an earlier reset in that block raises, the transaction
signal is still closed. This does not eliminate the hazard of a reset
raising — some other token in that block still leaks when that happens —
it only chooses, deliberately, which one leaks: a stuck ``True`` here
defers every later bump in that context, the most consequential token to
leave open.

Public backend-author contract
-------------------------------
A third-party unit of work that owns a transaction another mechanism will
later commit **must** call :func:`open_atomic_transaction` in its ``begin``
(or equivalent) and :func:`close_atomic_transaction` in its teardown,
symmetric with the openers above. Skipping this is silent: nothing
raises, but every write inside that transaction has its cache invalidation
race the commit again (the bug this module exists to close). This is a
deliberate public surface, not an oversight of a missing ``__all__`` — a
backend author has no other way to opt into deferral.
"""

from __future__ import annotations

from contextvars import ContextVar, Token

__all__ = [
    "close_atomic_transaction",
    "in_atomic_transaction",
    "open_atomic_transaction",
]

_in_atomic_transaction: ContextVar[bool] = ContextVar(
    "_loom_in_atomic_transaction",
    default=False,
)


def open_atomic_transaction() -> Token[bool]:
    """Mark an atomic transaction as open for the current context.

    Returns:
        Token to pass to :func:`close_atomic_transaction`.
    """
    return _in_atomic_transaction.set(True)


def close_atomic_transaction(token: Token[bool]) -> None:
    """Restore the state that was active before :func:`open_atomic_transaction`.

    Reset this first among a teardown's several resets: see the module
    docstring on why closing first only chooses which token leaks, and why
    this is the one that must be it.

    Args:
        token: Token returned by :func:`open_atomic_transaction`.
    """
    _in_atomic_transaction.reset(token)


def in_atomic_transaction() -> bool:
    """Whether an atomic transaction that will commit later is open."""
    return _in_atomic_transaction.get()
