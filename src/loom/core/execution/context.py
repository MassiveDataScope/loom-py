"""The execution in progress: who it runs as and what it has built.

An *execution* is one run of a use case, a job or an agent turn. Each one gets
an :class:`ExecutionContext`, published in a :class:`~contextvars.ContextVar`
while it runs. Handles read its identity through :func:`execution_identity`
when they are called, never when they are built (spec 011, D2), and a
``REQUEST`` binding is built once per execution through its cache (D4). The
rules live where they apply: inheritance and fresh contexts in :func:`enter`,
the token pair in :func:`leave`, failing closed in :func:`execution_identity`,
singleton construction in :class:`NoExecution`.

Concurrency:
    The context follows :mod:`contextvars`: a task created during an execution
    (each child of an ``asyncio.gather`` included) copies the context, so it
    sees the same identity and shares the same cache, and keeps both if it
    outlives the execution. ``asyncio.to_thread`` copies the context too;
    ``loop.run_in_executor`` does not, so a handle called there fails closed
    with :class:`ExecutionRequired`.
"""

from __future__ import annotations

from collections.abc import Hashable
from contextvars import ContextVar, Token
from types import TracebackType
from typing import cast

from loom.core.errors import Unauthenticated
from loom.core.execution.errors import ExecutionRequired
from loom.core.identity import Identity

_NO_EXECUTION_MESSAGE = (
    "The execution identity was read outside an execution. Call this from a use case, "
    "a job or an agent run, not while building a singleton or importing a module."
)
_NO_CALLER_MESSAGE = (
    "Authentication required: the execution in progress runs with no caller "
    "(such as a job callback)."
)
_MISUSED_TOKEN_MESSAGE = (
    "Execution token misuse: a token must be left once, in reverse order of entering; "
    "the execution context was closed."
)
_LEFT_OPEN_MESSAGE = (
    "An execution was left open or misused inside NoExecution; the execution context was closed."
)


class ExecutionContext:
    """The state of one execution: its identity and its per-execution cache.

    Built by :func:`enter`; code reads it through :func:`current_execution`.
    The cache is allocated on the first :meth:`store`, so an execution that
    builds no ``REQUEST`` binding allocates nothing beyond this object.

    Example::

        context = current_execution()
        if context is not None:
            client = context.get(AcmeClient)
            if client is None:
                client = AcmeClient()
                context.store(AcmeClient, client)
    """

    __slots__ = ("_cache", "_identity", "_token")

    def __init__(self, identity: Identity | None) -> None:
        self._identity = identity
        self._cache: dict[Hashable, object] | None = None
        self._token: Token[ExecutionContext | None] | None = None

    @property
    def identity(self) -> Identity | None:
        """The identity this execution runs as, or ``None`` for no caller.

        Read-only: it is fixed by :func:`enter`, and every task copied from
        this execution shares this object, so a reassignment would change the
        caller of all of them.
        """
        return self._identity

    def get(self, key: Hashable, default: object = None) -> object:
        """Return what this execution stored under ``key``.

        Args:
            key: The key given to :meth:`store`.
            default: Returned when nothing is stored under ``key``; pass a
                sentinel of your own when ``None`` is a value you store.

        Returns:
            The stored value, or ``default``.
        """
        cache = self._cache
        return default if cache is None else cache.get(key, default)

    def store(self, key: Hashable, value: object) -> None:
        """Keep ``value`` under ``key`` until this execution's context is gone.

        The value is shared with every task copied from this execution,
        including one that outlives it.

        Args:
            key: A binding-key object (a type or a private key instance),
                never a bare string another library could also pick.
            value: The value to keep.
        """
        cache = self._cache
        if cache is None:
            self._cache = {key: value}
        else:
            cache[key] = value


_current: ContextVar[ExecutionContext | None] = ContextVar("loom_execution", default=None)
_CLOSED = ExecutionContext(None)
"""Installed by a token misuse: reads as no execution, and no reset restores over it."""


def current_execution() -> ExecutionContext | None:
    """Return the execution in progress, or ``None`` outside any execution.

    A context closed by a token misuse also returns ``None``.

    Returns:
        The current :class:`ExecutionContext`, or ``None``.
    """
    context = _current.get()
    return None if context is _CLOSED else context


def enter(identity: Identity | None) -> Token[ExecutionContext | None]:
    """Open a new execution that runs as ``identity``.

    No inheritance happens here (NS2): ``identity`` is installed as given, even
    inside another execution. The executor computes the effective identity
    (explicit, else the enclosing execution's, else ``None``) once per
    execution. ``None`` means no caller: job callbacks run under
    ``enter(None)`` (D7). Every call builds a new :class:`ExecutionContext`,
    so a nested execution gets a fresh cache and shares no ``REQUEST``
    instance with the one around it (L12).

    Args:
        identity: The effective identity, computed by the caller.

    Returns:
        The token to pass to :func:`leave` in a ``finally`` block.

    Example::

        token = enter(identity)
        try:
            await acme_use_case.execute()
        finally:
            leave(token)
    """
    context = ExecutionContext(identity)
    token = _current.set(context)
    context._token = token  # pyright: ignore[reportPrivateUsage]
    return token


def leave(token: Token[ExecutionContext | None]) -> None:
    """Close the execution ``token`` opened and restore the one before it.

    Leave every token exactly once, in reverse order of entering, in the
    :mod:`contextvars` context that entered it: a ``finally`` block does all
    three.

    Args:
        token: The token returned by the matching :func:`enter`.

    Raises:
        RuntimeError: If ``token`` is not the token of the execution in
            progress (left twice, out of order, or with an execution entered
            over it and not left). The current :mod:`contextvars` context is
            then *closed*: it reads as no execution, and no later ``leave``
            or :class:`NoExecution` exit can bring a caller back.
        ValueError: If ``token`` was created in another :mod:`contextvars`
            context (a task that copied it); nothing changes in either.
    """
    context = _current.get()
    if context is None or context._token is not token:  # pyright: ignore[reportPrivateUsage]
        _current.set(_CLOSED)
        raise RuntimeError(_MISUSED_TOKEN_MESSAGE)
    _current.reset(token)
    context._token = None  # pyright: ignore[reportPrivateUsage]


def execution_identity() -> Identity:
    """Return the identity of the execution in progress, failing closed.

    It never falls back to :func:`~loom.core.identity.current_identity`: the
    ambient identity and the execution identity are separate on purpose.

    Returns:
        The identity, :data:`~loom.core.identity.ANONYMOUS` included.

    Raises:
        ExecutionRequired: Outside any execution, even while
            :func:`~loom.core.identity.set_identity` is active, and in a
            context closed by a token misuse.
        Unauthenticated: When the execution runs with no caller. Refusing
            ``ANONYMOUS`` is the consumer's rule.
    """
    context = _current.get()
    if context is None:
        raise ExecutionRequired(_NO_EXECUTION_MESSAGE)
    identity = context._identity  # pyright: ignore[reportPrivateUsage]
    if identity is None:
        # The closed sentinel has no identity: checked here, off the success path.
        if context is _CLOSED:
            raise ExecutionRequired(_NO_EXECUTION_MESSAGE)
        raise Unauthenticated(_NO_CALLER_MESSAGE)
    return identity


class ExecutionScope:
    """Run a ``with`` body as an execution with the given identity.

    The class form of :func:`enter` / :func:`leave` for code off the per-call
    hot path: an agent run, or building a ``REQUEST`` binding outside any
    execution. Entering an open instance again raises :class:`RuntimeError`.

    Args:
        identity: The identity the body runs as; ``None`` for no caller.

    Example::

        with ExecutionScope(acme_identity):
            reply = acme_agent.ask("Summarise invoice 42")
    """

    __slots__ = ("_identity", "_token")

    def __init__(self, identity: Identity | None) -> None:
        self._identity = identity
        self._token: Token[ExecutionContext | None] | None = None

    def __enter__(self) -> None:
        if self._token is not None:
            raise RuntimeError("This ExecutionScope is already open")
        self._token = enter(self._identity)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        token = cast("Token[ExecutionContext | None]", self._token)
        self._token = None
        leave(token)


class NoExecution:
    """Run a ``with`` body outside any execution, even inside one.

    Singletons are built here, so a constructor that reads the execution
    identity raises :class:`ExecutionRequired` instead of freezing whichever
    caller built it first. The enclosing execution is back after the body,
    unless the body left an execution open or misused a token: then the block
    closes the context and raises :class:`RuntimeError`, since the enclosing
    execution may be the one a misused token already tried to leave.

    Example::

        with NoExecution():
            acme_client = AcmeClient()
    """

    __slots__ = ("_token",)

    def __init__(self) -> None:
        self._token: Token[ExecutionContext | None] | None = None

    def __enter__(self) -> None:
        if self._token is not None:
            raise RuntimeError("This NoExecution is already open")
        self._token = _current.set(None)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        token = cast("Token[ExecutionContext | None]", self._token)
        self._token = None
        if _current.get() is not None:
            _current.set(_CLOSED)
            raise RuntimeError(_LEFT_OPEN_MESSAGE)
        _current.reset(token)
