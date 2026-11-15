"""The execution in progress: the resources it owns, and the identity it fixes.

An *execution* is one run of a use case, a job or an agent turn. Each one gets
an :class:`ExecutionContext` holding what it owns: the per-execution cache
through which a ``REQUEST`` binding is built once per execution. It is
published as the ``work`` of the acting identity of :mod:`loom.core.identity`,
beside who it acts as: :meth:`ExecutionContext.entered` fixes both, for the execution and every
task it starts, in one value. Handles read the identity through
:func:`execution_identity` when they are called, never when they are built.

This module writes the carrier of :mod:`loom.core.identity` (``_acting``)
itself: to enter an execution with
:meth:`~loom.core.identity.Acting.pinned_for`, to close it, to act as no one
in :class:`NoExecution`, and to close a context after a token misuse.

Concurrency:
    The context follows :mod:`contextvars`: a task created during an execution
    (each child of an ``asyncio.gather`` or a ``TaskGroup`` included) copies
    the context, so it acts as the same identity and shares the same cache
    while the execution runs. ``asyncio.to_thread`` copies the context too;
    ``loop.run_in_executor`` does not, so a handle called there fails closed
    with :class:`ExecutionRequired`.

Lifetime:
    The identity governs everything the execution causes, for its whole life;
    the resources last as long as the execution. :meth:`ExecutionContext.close` ends them: a
    task or thread still holding a copy afterwards keeps acting as the same
    caller (:func:`execution_identity`, ``Caller()``, row-level security) but
    reads no per-execution value: that raises :class:`ExecutionClosed` and
    logs the supported paths, run a use case or dispatch a job, which act as
    the same caller in resources of their own (an ended execution is no
    execution, so :func:`current_execution` returns ``None`` there). Work
    awaited inside the execution has ended before it closes. Such a task acts
    as its caller only within its delegation: past the deadline,
    :meth:`ExecutionContext.entered` refuses an execution it opens, and
    :func:`execution_identity` refuses to read its caller.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Hashable
from contextvars import Token
from types import TracebackType
from typing import Any, cast

from loom.core.errors import Unauthenticated
from loom.core.execution.errors import ExecutionClosed, ExecutionRequired
from loom.core.identity import Acting, Identity
from loom.core.identity.context import _acting  # pyright: ignore[reportPrivateUsage]
from loom.core.logger import get_logger

_log = get_logger(__name__)

_acting_now = _acting.get

_NO_EXECUTION_MESSAGE = (
    "The execution identity was read outside an execution. Call this from a use case, "
    "a job or an agent run, not while building a singleton or importing a module."
)
_NO_CALLER_MESSAGE = (
    "Authentication required: the execution in progress acts with no caller: nothing was "
    "given and nothing was acting when it started. Pass identity=... to the executor "
    "(ANONYMOUS to run without a caller)."
)
_MISUSED_TOKEN_MESSAGE = (
    "Execution misuse: an execution must be closed once, in reverse order of entering; "
    "the execution context was closed."
)
_LEFT_OPEN_MESSAGE = (
    "An execution was left open or misused inside NoExecution; the execution context was closed."
)
_BACKGROUND_WORK = (
    "Run a use case (executor.run(UseCase)) or dispatch a job (JobService.dispatch) from "
    "the task: they act as the same caller, in resources of their own."
)
_ENDED_MESSAGE = (
    "The execution this task or thread copied its context from has ended: its resources "
    "(per-execution values, unit of work, post-commit channel) are gone. " + _BACKGROUND_WORK
)


class ExecutionContext:
    """The resources of one execution: its per-execution cache.

    Opened by :meth:`entered`; code reads it through :func:`current_execution`.
    The cache is allocated on the first store, so an execution that builds
    no ``REQUEST`` binding allocates nothing beyond this object. It holds
    what :meth:`store` and :func:`per_execution` keep, each value under its
    key, and, for each key :func:`per_execution` has built, the reentrant
    lock it builds under, keyed ``(_BUILD_LOCK_TAG, key)`` with a private tag no
    caller's key can equal. :meth:`close` ends it: the cache and every read
    of it are gone (:class:`ExecutionClosed`). It holds no identity: who the
    execution acts as is the acting identity :meth:`entered` fixed with it.

    Attributes:
        ended: Whether :meth:`close` closed this execution.

    Example::

        context = current_execution()
        if context is not None:
            client = context.get(AcmeClient)
            if client is None:
                client = AcmeClient()
                context.store(AcmeClient, client)
    """

    __slots__ = ("_cache", "_token", "ended")

    def __init__(self) -> None:
        self._cache: dict[Hashable, object] | None = None
        self._token: Token[Acting] | None = None
        self.ended = False

    @classmethod
    def entered(cls, identity: Identity | None, what: str = "execution") -> ExecutionContext:
        """Open a new execution acting as ``identity``, and fix it for the execution's work.

        ``set_identity`` raises :class:`~loom.core.identity.IdentityFixed` in
        the execution and in every task it starts. ``identity`` is the
        effective one, computed by the caller; ``None`` means no caller. Each
        execution has a cache of its own, shared with no execution around it.
        The switch rule is :meth:`~loom.core.identity.Acting.pinned_for`'s: the
        same subject keeps the delegation and is refused past it; another
        subject is a logged switch with a delegation of its own.

        Args:
            identity: The effective identity, computed by the caller.
            what: What runs, for the switch report (the use case's name).

        Returns:
            The execution, to :meth:`close` in a ``finally`` block.

        Raises:
            DelegationExpired: Acting as the subject acting, past its deadline.

        Example::

            execution = ExecutionContext.entered(identity, "AcmeUseCase")
            try:
                await acme_use_case.execute()
            finally:
                execution.close()
        """
        context = cls()
        context._token = _acting.set(_acting_now().pinned_for(identity, context, what))
        return context

    def close(self) -> None:
        """End this execution and restore the work and identity before it.

        Close every execution exactly once, in reverse order of entering, in
        the :mod:`contextvars` context that entered it: a ``finally`` block
        does all three. A task or thread that copied its context and reads
        the cache afterwards gets :class:`ExecutionClosed`, and keeps acting
        as the same caller.

        Raises:
            RuntimeError: When this is not the execution in progress (closed
                twice, out of order, or with an execution entered over it and
                not closed). The current :mod:`contextvars` context is then
                closed for good: it reads as no execution and acts as no one
                with its identity fixed.
            ValueError: When called from another :mod:`contextvars` context
                (a task that copied it); nothing changes in either.
        """
        token = self._token
        if token is None or _acting_now().work is not self:
            _acting.set(_CLOSED_CONTEXT_ACTING)
            raise RuntimeError(_MISUSED_TOKEN_MESSAGE)
        _acting.reset(token)
        self._token = None
        self._cache = _ENDED
        self.ended = True

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
            with _CACHE_ALLOCATION_LOCK:
                cache = self._cache
                if cache is None:
                    cache = self._cache = {}
        return cache


_CACHE_ALLOCATION_LOCK = threading.Lock()
"""Taken only to allocate an execution's cache, once per execution that stores anything."""


class _EndedCache(dict[Hashable, object]):
    """The cache of every execution that ended: any read or write raises :class:`ExecutionClosed`.

    :meth:`ExecutionContext.close` installs the one instance, :data:`_ENDED`,
    in place of the cache.
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


_BUILD_LOCK_TAG = object()
"""Tags the key of a per-execution build lock in the cache: ``(_BUILD_LOCK_TAG, key)``."""

_NO_EXECUTION = ExecutionContext()
"""The work of :class:`NoExecution` and of a closed context: no execution, and no caller, fixed."""
_NO_EXECUTION_ACTING = Acting.delegated(None, work=_NO_EXECUTION)
_CLOSED_CONTEXT_ACTING = Acting.delegated(None, work=_NO_EXECUTION)
"""Installed after a token misuse: no execution, no caller, and never restored.

A value of its own, not :data:`_NO_EXECUTION_ACTING`: no later close or
:class:`NoExecution` exit can bring back an execution or a caller a misuse
may have ended.
"""


def current_execution() -> ExecutionContext | None:
    """Return the execution in progress, or ``None`` outside any execution.

    A context closed by a token misuse, and an execution that ended (seen
    from a task that outlived it), also return ``None``: neither is an
    execution in progress.

    Returns:
        The current :class:`ExecutionContext`, or ``None``.
    """
    context: Any = _acting_now().work
    if context is None or context is _NO_EXECUTION or context.ended:
        return None
    return context


def per_execution(key: Hashable, make: Callable[[], object]) -> Callable[[], object]:
    """Return a callable giving one value of *make* per execution, kept under *key*.

    Inside an execution the value is built once, kept in its cache and
    returned to everything that execution runs, its tasks and copied threads
    included; outside any execution, or in a context closed by a token
    misuse, each call builds a fresh one. A task or thread calling it after
    its execution ended gets :class:`ExecutionClosed`.

    A miss builds under a reentrant lock of this execution and *key*, kept
    in the cache from the key's first build, so threads sharing one
    execution (``asyncio.to_thread`` copies its context) wait for the one
    build of a key while builds of other keys proceed: *make* may need
    another per-execution value, on its thread or on another one it waits
    for. Two threads each waiting for the other's build of the same key
    wait forever, as a cycle of providers does.
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
    building = (_BUILD_LOCK_TAG, key)

    def value() -> object:
        context: Any = _acting_now().work
        if context is None:
            return make()
        cache = context._cache
        if cache is not None and key in cache:
            return cache[key]
        if context is _NO_EXECUTION:
            return make()
        cache = context._allocated()
        lock: Any = cache.get(building)
        if lock is None:
            lock = cache.setdefault(building, threading.RLock())
        with lock:
            if key not in cache:
                cache[key] = make()
            return cache[key]

    return value


def execution_ended() -> bool:
    """Whether this context was copied from an execution that has since ended.

    Such a task or thread is in no execution: what it does runs as at top
    level, acting as the same caller.

    Returns:
        ``True`` in a task or thread whose execution ended.
    """
    context: Any = _acting_now().work
    return context is not None and context.ended


def execution_identity() -> Identity:
    """Return the identity the execution in progress acts as, failing closed.

    That is the acting identity :meth:`ExecutionContext.entered` fixed, which a task the
    execution started keeps after the execution ended, within its
    delegation.

    Returns:
        The identity, :data:`~loom.core.identity.ANONYMOUS` included.

    Raises:
        ExecutionRequired: Outside any execution, even while
            :func:`~loom.core.identity.set_identity` is active, and in a
            context closed by a token misuse.
        Unauthenticated: When the execution acts with no caller, or, read
            from a task whose execution ended, past the caller's deadline.
            Refusing ``ANONYMOUS`` is the consumer's rule.
    """
    acting = _acting_now()
    identity, context = acting.caller, acting.work
    if identity is None or context is None:
        raise _no_identity(context)
    if context.ended:  # pyright: ignore[reportAttributeAccessIssue]
        acting.refuse_expired()
    return identity


def live_execution_identity() -> Identity:
    """Return the identity the execution in progress acts as, refused past its deadline.

    Returns:
        The identity, :data:`~loom.core.identity.ANONYMOUS` included.

    Raises:
        ExecutionRequired: Outside any execution, and in a context closed by
            a token misuse.
        Unauthenticated: When the execution acts with no caller.
        DelegationExpired: Past the caller's deadline.
    """
    acting = _acting_now()
    identity, context = acting.caller, acting.work
    if identity is None or context is None:
        raise _no_identity(context)
    acting.refuse_expired()
    return identity


def _no_identity(context: object) -> ExecutionRequired | Unauthenticated:
    """The error of an identity read outside an execution or in one acting with no caller."""
    if context is None or context is _NO_EXECUTION:
        return ExecutionRequired(_NO_EXECUTION_MESSAGE)
    return Unauthenticated(_NO_CALLER_MESSAGE)


class ExecutionScope:
    """Run a ``with`` body as an execution acting as the given identity.

    The class form of :meth:`ExecutionContext.entered` and
    :meth:`ExecutionContext.close` for code off the per-call hot path: an
    agent run, or building a ``REQUEST`` binding outside any execution. Acting
    as another identity than the one acting is an explicit switch, by
    :meth:`ExecutionContext.entered`'s rule, as ``executor.run(identity=...)``:
    logged.
    Entering an open instance again raises :class:`RuntimeError`.

    Args:
        identity: The identity the body runs as; ``None`` for no caller.
        what: What the body runs, for the switch report.

    Example::

        with ExecutionScope(acme_identity):
            reply = acme_agent.ask("Summarise invoice 42")
    """

    __slots__ = ("_execution", "_identity", "_what")

    def __init__(self, identity: Identity | None, *, what: str = "ExecutionScope") -> None:
        self._identity = identity
        self._what = what
        self._execution: ExecutionContext | None = None

    def __enter__(self) -> None:
        if self._execution is not None:
            raise RuntimeError("This ExecutionScope is already open")
        self._execution = ExecutionContext.entered(self._identity, self._what)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        execution = self._execution
        if execution is None:
            raise RuntimeError("This ExecutionScope is not open")
        self._execution = None
        execution.close()


class NoExecution:
    """Run a ``with`` body outside any execution, acting as no one, even inside one.

    Singletons are built here: a constructor reading the execution identity
    raises :class:`ExecutionRequired`, and one reading the acting identity
    sees :data:`~loom.core.identity.ANONYMOUS`. The enclosing execution and
    its caller are back after the body, unless the body left an execution
    open or misused a token: then the block closes the context and raises
    :class:`RuntimeError`.

    Example::

        with NoExecution():
            acme_client = AcmeClient()
    """

    __slots__ = ("_token",)

    def __init__(self) -> None:
        self._token: Token[Acting] | None = None

    def __enter__(self) -> None:
        if self._token is not None:
            raise RuntimeError("This NoExecution is already open")
        self._token = _acting.set(_NO_EXECUTION_ACTING)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        token = cast("Token[Acting]", self._token)
        self._token = None
        if _acting_now() is not _NO_EXECUTION_ACTING:
            _acting.set(_CLOSED_CONTEXT_ACTING)
            raise RuntimeError(_LEFT_OPEN_MESSAGE)
        _acting.reset(token)
