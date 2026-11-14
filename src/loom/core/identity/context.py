"""The acting identity: who the work in progress acts as, until when, and for which work.

One carrier, a ``contextvars.ContextVar``, holds :data:`Acting`. A task, a
thread started with ``asyncio.to_thread`` and anything else that copies the
context inherits it, for its whole life: the identity of a request governs
everything that request causes, including work that outlives it.

Rules (spec 011, T09a):
    - A caller always has a deadline: the end of its delegation, ``now +
      identity.max_delegation_seconds`` from where it started. The window is
      configured once (:func:`configure_delegation`).
    - A transport installs the caller it verified with :func:`set_identity`
      (the HTTP request starts a delegation; a job envelope or a queued
      dispatch resumes one) and restores the previous one with
      :func:`reset_identity`. It never fixes the identity.
    - Work fixes it: :func:`fix_identity` is called by
      :func:`~loom.core.execution.enter` only, and applies the one rule of a
      switch. Inside work :func:`set_identity` raises :class:`IdentityFixed`.
      The execution layer also reads and writes the carrier itself
      (``_acting``): to leave an execution, for ``NoExecution`` and to close
      a context after a token misuse. That leak is deliberate and fenced by
      ``tests/unit/test_layers.py``.
    - Expiry is checked where work starts (:func:`fix_identity`), at the
      start of every SQLAlchemy transaction and when a task whose execution
      ended reads its caller (:func:`refuse_expired_delegation`), never by
      :func:`current_identity`: work started before the deadline may finish.
    - Every explicit switch (``executor.run(..., identity=X)``,
      ``invoker.for_identity(X)``, ``ExecutionScope(X)``, a transport
      replacing the caller) and every refusal past a deadline is reported
      here: an ``identity_switched`` or ``delegation_expired`` warning, and
      span attributes for a switch. The logger and OpenTelemetry are
      imported on first use, not with this layer.
    - This module imports nothing from the execution, the engine, the
      container or the use cases.
"""

from __future__ import annotations

import math
import time
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING

from loom.core.errors import IdentityFixed, Unauthenticated
from loom.core.identity.identity import ANONYMOUS, Identity

if TYPE_CHECKING:
    from loom.core.logger import LoggerPort

Acting = tuple[Identity, Identity | None, float, object | None]
"""``(shown, caller, deadline, work)``: the value of the acting identity.

``caller`` is the acting identity, ``None`` for no caller; ``shown`` is the
same with ``ANONYMOUS`` for ``None``, so :func:`current_identity` is one
index. ``deadline`` (a :func:`time.time` value) ends the caller's
delegation: finite while a caller acts, ``inf`` with none. ``work`` is the
work that fixed the identity, opaque here; ``None`` at transport level.
"""

DEFAULT_MAX_DELEGATION_SECONDS = 3600.0
"""Default of ``identity.max_delegation_seconds``: one hour of delegated work per request."""

_NO_ONE: Acting = (ANONYMOUS, None, math.inf, None)
_acting: ContextVar[Acting] = ContextVar("loom_acting_identity", default=_NO_ONE)
_window = DEFAULT_MAX_DELEGATION_SECONDS

_FIXED_MESSAGE = (
    "The identity is fixed for the work in progress and every task it started: only an "
    "explicit switch changes it, executor.run(UseCase, identity=...), "
    "invoker.for_identity(...) or ExecutionScope(...), which is logged."
)

_DELEGATION_EXPIRED_MESSAGE = (
    "Authentication required: the delegation of this caller has expired. Work a request "
    "causes acts as its caller for at most identity.max_delegation_seconds after the "
    "request started; run later work as a system identity, switching explicitly with "
    "executor.run(UseCase, identity=...)."
)

_log: LoggerPort | None = None
"""Built on the first switch or refusal; a test may install a recorder in its place."""


def configure_delegation(seconds: float) -> None:
    """Set ``identity.max_delegation_seconds`` for the process: its one validation point.

    The composition reads the setting and passes it here.

    Args:
        seconds: How long work a request causes may act as its caller.

    Raises:
        ConfigError: When *seconds* is not a finite number above zero.
    """
    if not (seconds > 0 and math.isfinite(seconds)):
        from loom.core.config import ConfigError  # the refusal only: config is heavy to import

        raise ConfigError(
            f"identity.max_delegation_seconds must be a finite number above zero, got {seconds}"
        )
    global _window
    _window = seconds


def current_identity() -> Identity:
    """Return the acting identity, or :data:`~loom.core.identity.identity.ANONYMOUS`.

    Never returns ``None``: with no caller it yields ``ANONYMOUS``, so
    consumers cannot accidentally treat "unknown" as "authorized".

    Returns:
        The acting identity, or the anonymous one.
    """
    return _acting.get()[0]


def current_caller() -> Identity | None:
    """Return the acting identity, or ``None`` when no caller acts.

    ``None`` (no caller) differs from
    :data:`~loom.core.identity.identity.ANONYMOUS` (an explicit anonymous
    caller).

    Returns:
        The acting identity, or ``None``.
    """
    return _acting.get()[1]


def set_identity(identity: Identity | None, *, until: float | None = None) -> Token[Acting]:
    """Install the caller a transport verified, for the current context.

    Replacing a different caller is a switch, logged
    (``identity_switched``).

    Args:
        identity: Verified identity produced by an authentication mechanism;
            ``None`` installs no caller.
        until: The deadline of the delegation it resumes (a job envelope, a
            queued dispatch), clamped to ``now +
            identity.max_delegation_seconds``; none starts one of that
            length (a request).

    Returns:
        A :class:`~contextvars.Token` that must be passed to
        :func:`reset_identity` in a ``finally`` block.

    Raises:
        IdentityFixed: Inside work, including a task that outlived it.

    Example::

        token = set_identity(identity)
        try:
            await handle_request()
        finally:
            reset_identity(token)
    """
    if identity is None:
        deadline = math.inf
    elif until is None:
        deadline = time.time() + _window
    else:  # never past one window, whatever a message claims (nan stays expired)
        deadline = min(until, time.time() + _window)
    token = _acting.set((identity or ANONYMOUS, identity, deadline, None))
    # The value replaced, read off the token: no extra read on the request path.
    previous = token.old_value
    if previous is Token.MISSING:
        return token
    _, caller, _, work = previous
    if work is not None:
        _acting.reset(token)
        raise IdentityFixed(_FIXED_MESSAGE)
    if caller is not None and caller is not identity and caller != identity:
        try:
            _report_switch(caller, identity, "transport")
        except BaseException:
            _acting.reset(token)  # no switch half done: the replaced caller still acts
            raise
    return token


def reset_identity(token: Token[Acting]) -> None:
    """Restore the identity active before the matching :func:`set_identity`.

    Args:
        token: Token returned by the corresponding :func:`set_identity` call.
    """
    _acting.reset(token)


def fix_identity(identity: Identity | None, work: object, what: str) -> Token[Acting]:
    """Fix *identity* for *work*, over the acting ``(caller, deadline)``: the one rule of a switch.

    Called by :func:`~loom.core.execution.enter` only.

    - The same subject as the caller acting keeps its deadline, and is
      refused once it has passed; other roles or attributes are a switch.
    - Another subject is a switch with a new delegation, ``now + window``.
    - No caller has no deadline.

    A switch is logged when someone was acting or *work* is nested in work.

    Args:
        identity: The identity the work acts as; ``None`` for no caller.
        work: The work that fixes it; opaque here.
        what: What runs, for the switch report (the use case's name).

    Returns:
        The token the execution resets when the work ends
        (:func:`~loom.core.execution.leave`).

    Raises:
        Unauthenticated: Acting as the subject acting past its deadline.
    """
    _, caller, deadline, outer = _acting.get()
    if identity is None:
        deadline = math.inf
    elif caller is None or identity.subject != caller.subject:
        deadline = time.time() + _window
    elif not time.time() <= deadline:  # nan has expired too
        raise _delegation_expired()
    if identity is not caller and identity != caller and (caller is not None or outer is not None):
        _report_switch(caller, identity, what)
    return _acting.set((identity or ANONYMOUS, identity, deadline, work))


def delegation() -> tuple[Identity | None, float | None]:
    """The caller acting and its deadline, as work dispatched now captures them.

    A plain read: ``(None, None)`` unless an authenticated caller acts. An
    unauthenticated caller (``ANONYMOUS``) is captured as no caller, the job
    wire's policy, in every backend.
    """
    _, caller, deadline, _ = _acting.get()
    if caller is None or not caller.is_authenticated:
        return None, None
    return caller, deadline


def refuse_expired_delegation() -> None:
    """Refuse work acting as a caller past its deadline.

    For code acting for the caller without entering work: the start of
    every SQLAlchemy transaction (row-level security and audit columns
    alike), and an execution's identity read from a task that outlived it.

    Raises:
        Unauthenticated: When a caller acts past its deadline.
    """
    if not time.time() <= _acting.get()[2]:  # nan has expired too
        raise _delegation_expired()


def _logger() -> LoggerPort:
    """The module's logger, built on the first switch or refusal: not imported with the layer."""
    global _log
    if _log is None:
        from loom.core.logger import get_logger

        _log = get_logger(__name__)
    return _log


def _report_switch(previous: Identity | None, identity: Identity | None, what: str) -> None:
    """Log that *what* acts as *identity* instead of *previous*, and record it on the span.

    Subjects and roles only, never an identity's attributes or credentials:
    ``attributes_changed`` flags two callers whose attributes differ, which
    is how a switch keeping subject and roles shows.

    Args:
        previous: The identity acting before the switch; ``None`` for no
            caller (a switch inside work that had none).
        identity: The identity acting from it on; ``None`` for no caller.
        what: What runs as *identity*: the use case, ``ExecutionScope`` or
            ``transport``.
    """
    from loom.core.observability.span import annotate_current_span
    from loom.core.tracing import get_trace_id

    from_subject = None if previous is None else previous.subject
    to_subject = None if identity is None else identity.subject
    _logger().warning(
        "identity_switched",
        from_subject=from_subject,
        to_subject=to_subject,
        from_roles=None if previous is None else previous.roles,
        to_roles=None if identity is None else identity.roles,
        attributes_changed=(
            previous is not None
            and identity is not None
            and previous.attributes != identity.attributes
        ),
        use_case=what,
        trace_id=get_trace_id(),
    )
    annotate_current_span(
        {
            "loom.identity.switched_from": from_subject or "",
            "loom.identity.switched_to": to_subject or "",
        }
    )


def _delegation_expired() -> Unauthenticated:
    """Log the refusal of work acting past its delegation, and return the error to raise.

    Raised where work starts as the caller (an execution, a job or callback
    run, a queued dispatch's job), at the start of a database transaction,
    and when a task whose execution ended reads its caller.
    """
    _logger().warning("delegation_expired", guidance=_DELEGATION_EXPIRED_MESSAGE)
    return Unauthenticated(_DELEGATION_EXPIRED_MESSAGE)
