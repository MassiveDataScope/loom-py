"""The execution in progress: who it runs as and what it has built.

An *execution* is one run of a use case, a job or an agent turn. Each one gets
an :class:`ExecutionContext`, published in a :class:`~contextvars.ContextVar`
while it runs. Handles read its identity through :func:`execution_identity`
when they are called, never when they are built (spec 011, D2), and a
``REQUEST`` binding is built once per execution through its cache (D4). The
rules live where they apply: inheritance and fresh contexts in :func:`enter`,
the token pair and closing in :func:`leave`, failing closed in
:func:`execution_identity`, singleton construction in :class:`NoExecution`.

Concurrency:
    The context follows :mod:`contextvars`: a task created during an execution
    (each child of an ``asyncio.gather`` or a ``TaskGroup`` included) copies
    the context, so it sees the same identity and shares the same cache while
    the execution runs. ``asyncio.to_thread`` copies the context too;
    ``loop.run_in_executor`` does not, so a handle called there fails closed
    with :class:`ExecutionRequired`.

Lifetime:
    The execution owns its context: :func:`leave` closes it. A task or a
    thread still holding a copy afterwards reads neither its identity nor a
    per-execution value, nor dispatch a job (:func:`refuse_ended_execution`):
    each raises :class:`ExecutionClosed` and logs the supported paths for
    background work: dispatch a job from inside the execution, or call
    ``executor.run(UseCase, identity=...)`` from the task, which opens its
    own execution (an ended execution is no execution, so
    :func:`current_execution` returns ``None`` there). Work awaited inside
    the execution has ended before it closes.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Hashable
from contextvars import ContextVar, Token
from types import TracebackType
from typing import Any, cast

from loom.core.errors import Unauthenticated
from loom.core.execution.errors import ExecutionClosed, ExecutionRequired
from loom.core.identity import Identity
from loom.core.logger import get_logger

_log = get_logger(__name__)

_NO_EXECUTION_MESSAGE = (
    "The execution identity was read outside an execution. Call this from a use case, "
    "a job or an agent run, not while building a singleton or importing a module."
)
_NO_CALLER_MESSAGE = (
    "Authentication required: the execution in progress runs with no caller "
    "(such as a job callback, or executor.run given no identity=... from a task "
    "whose execution ended)."
)
_MISUSED_TOKEN_MESSAGE = (
    "Execution token misuse: a token must be left once, in reverse order of entering; "
    "the execution context was closed."
)
_LEFT_OPEN_MESSAGE = (
    "An execution was left open or misused inside NoExecution; the execution context was closed."
)
_BACKGROUND_WORK = (
    "Background work runs in an execution of its own: dispatch a job from inside the "
    "execution (JobService.dispatch), or call executor.run(UseCase, identity=...) from "
    "the task, which opens its own execution."
)
_ENDED_MESSAGE = (
    "The execution this task or thread copied its context from has ended: its identity "
    "and per-execution values are gone. " + _BACKGROUND_WORK
)


class ExecutionContext:
    """The state of one execution: its identity and its per-execution cache.

    Built by :func:`enter`; code reads it through :func:`current_execution`.
    The cache is allocated on the first store, so an execution that builds
    no ``REQUEST`` binding allocates nothing beyond this object. It holds
    what :meth:`store` and :func:`per_execution` keep, each value under its
    key, and, for each key :func:`per_execution` has built, the reentrant
    lock it builds under, keyed ``(_BUILDING, key)`` with a private tag no
    caller's key can equal. :func:`leave` closes it: the identity, the cache
    and every read of either are gone (:class:`ExecutionClosed`).

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

        Raises:
            ExecutionClosed: Once the execution has ended.
        """
        identity = self._identity
        if identity is None and self._cache is _ENDED:
            raise _closed("identity")
        return identity

    def get(self, key: Hashable, default: object = None) -> object:
        """Return what this execution stored under ``key``.

        Args:
            key: The key given to :meth:`store`.
            default: Returned when nothing is stored under ``key``; pass a
                sentinel of your own when ``None`` is a value you store.

        Returns:
            The stored value, or ``default``.

        Raises:
            ExecutionClosed: Once the execution has ended.
        """
        cache = self._cache
        return default if cache is None else cache.get(key, default)

    def store(self, key: Hashable, value: object) -> None:
        """Keep ``value`` under ``key`` until this execution ends.

        The value is shared with every task copied from this execution while
        it runs.

        Args:
            key: A binding-key object (a type or a private key instance),
                never a bare string another library could also pick.
            value: The value to keep.

        Raises:
            ExecutionClosed: Once the execution has ended.
        """
        self._allocated()[key] = value

    def _allocated(self) -> dict[Hashable, object]:
        """The cache, allocated once even when threads sharing this execution race for it."""
        cache = self._cache
        if cache is None:
            with _ALLOCATING:
                cache = self._cache
                if cache is None:
                    cache = self._cache = {}
        return cache


_ALLOCATING = threading.Lock()
"""Taken only to allocate an execution's cache, once per execution that stores anything."""


class _EndedCache(dict[Hashable, object]):
    """The cache of every execution that ended: any read or write raises :class:`ExecutionClosed`.

    :func:`leave` installs the one instance, :data:`_ENDED`, in place of the
    cache, so a closed execution is refused by the reads it already does:
    the hit path of :func:`per_execution` checks nothing more than before.
    """

    __slots__ = ()

    def __contains__(self, key: object) -> bool:
        raise _closed("per-execution value")

    def get(self, key: Hashable, default: object = None) -> object:  # type: ignore[override]
        raise _closed("per-execution value")

    def __setitem__(self, key: Hashable, value: object) -> None:
        raise _closed("per-execution value")


_ENDED = _EndedCache()


def _closed(read: str) -> ExecutionClosed:
    """Log the supported paths for background work and return the error to raise."""
    _log.warning("execution_closed", read=read, guidance=_BACKGROUND_WORK)
    return ExecutionClosed(_ENDED_MESSAGE)


_BUILDING = object()
"""Tags the key of a per-execution build lock in the cache: ``(_BUILDING, key)``."""

_current: ContextVar[ExecutionContext | None] = ContextVar("loom_execution", default=None)
_CLOSED = ExecutionContext(None)
"""Installed by a token misuse: reads as no execution, and no reset restores over it."""


def current_execution() -> ExecutionContext | None:
    """Return the execution in progress, or ``None`` outside any execution.

    A context closed by a token misuse, and an execution that ended (seen
    from a task that outlived it), also return ``None``: neither is an
    execution in progress.

    Returns:
        The current :class:`ExecutionContext`, or ``None``.
    """
    context = _current.get()
    if context is None or context is _CLOSED or context._cache is _ENDED:  # pyright: ignore[reportPrivateUsage]
        return None
    return context


def per_execution(key: Hashable, make: Callable[[], object]) -> Callable[[], object]:
    """Return a callable giving one value of *make* per execution, kept under *key*.

    Inside an execution the value is built once, kept in its cache and
    returned to everything that execution runs, its tasks and copied threads
    included; outside any execution, or in a context closed by a token
    misuse, each call builds a fresh one. A task or thread calling it after
    its execution ended gets :class:`ExecutionClosed`. The cache is read
    directly, with no call on the hit path.

    A miss builds under a reentrant lock of this execution and *key*, kept
    in the cache from the key's first build, so threads sharing one
    execution (``asyncio.to_thread`` copies its context) wait for the one
    build of a key while builds of other keys proceed: *make* may need
    another per-execution value, on its thread or on another one it waits
    for. Two threads each waiting for the other's build of the same key
    wait forever, as a cycle of providers does (a bug either way).
    ``loop.run_in_executor`` copies no context: its thread runs outside the
    execution. A singleton is built under ``NoExecution``, so it
    never waits for a per-execution build.

    Args:
        key: A binding-key object (a type or a private key instance), never
            a bare string another library could also pick.
        make: Builds the value.

    Raises:
        ExecutionClosed: From the callable, once its execution has ended.

    Example::

        acme_session = per_execution(AcmeSession, AcmeSession)
        with ExecutionScope(acme_identity):
            assert acme_session() is acme_session()
    """
    current = _current.get
    building = (_BUILDING, key)

    def value() -> object:
        context = current()
        if context is None:
            return make()
        cache = context._cache  # pyright: ignore[reportPrivateUsage]
        if cache is not None and key in cache:
            return cache[key]
        if context is _CLOSED:
            return make()
        cache = context._allocated()  # pyright: ignore[reportPrivateUsage]
        lock: Any = cache.get(building)
        if lock is None:
            # setdefault is atomic: threads racing here all get the one lock stored.
            lock = cache.setdefault(building, threading.RLock())
        with lock:
            if key not in cache:
                cache[key] = make()
            return cache[key]

    return value


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
    three. The execution's identity and cache are dropped: a task or thread
    that copied its context and reads either afterwards gets
    :class:`ExecutionClosed`.

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
    context._identity = None  # pyright: ignore[reportPrivateUsage]
    context._cache = _ENDED  # pyright: ignore[reportPrivateUsage]


def refuse_ended_execution(what: str) -> None:
    """Refuse *what* in a task or thread whose execution has ended, logging the guidance.

    For work that would land on what the ended execution owned, such as a
    job dispatch queued on its drained post-commit channel.

    Args:
        what: What is refused, for the log (``"job dispatch"``).

    Raises:
        ExecutionClosed: When the execution this context copied has ended.
    """
    context = _current.get()
    if context is not None and context._cache is _ENDED:  # pyright: ignore[reportPrivateUsage]
        raise _closed(what)


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
        ExecutionClosed: In a task or thread whose execution has ended.
        Unauthenticated: When the execution runs with no caller. Refusing
            ``ANONYMOUS`` is the consumer's rule.
    """
    context = _current.get()
    if context is None:
        raise ExecutionRequired(_NO_EXECUTION_MESSAGE)
    identity = context._identity  # pyright: ignore[reportPrivateUsage]
    if identity is None:
        # Closed and ended contexts have no identity: checked here, off the success path.
        if context is _CLOSED:
            raise ExecutionRequired(_NO_EXECUTION_MESSAGE)
        if context._cache is _ENDED:  # pyright: ignore[reportPrivateUsage]
            raise _closed("identity")
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
