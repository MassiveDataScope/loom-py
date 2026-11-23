"""The acting identity: who the work in progress acts as, until when, and for which work.

One carrier, a ``contextvars.ContextVar``, holds :class:`Acting`. A task, a
thread started with ``asyncio.to_thread`` and anything else that copies the
context inherits it, for its whole life: the identity of a request governs
everything that request causes, including work that outlives it.

Rules:
    - A caller always has a deadline: the end of its delegation, ``now +
      identity.max_delegation_seconds`` from where it started
      (:meth:`Acting.delegated`). The window is set with
      :func:`configure_delegation`.
    - A transport installs the caller it verified with :func:`set_identity`
      (the HTTP request starts a delegation; a job envelope or a queued
      dispatch resumes one) and restores the previous one with
      :func:`reset_identity`.
    - Work pins it: :meth:`~loom.core.execution.ExecutionContext.entered` installs
      :meth:`Acting.pinned_for`. Inside work :func:`set_identity` raises
      :class:`IdentityFixed`. The execution layer also writes the carrier
      (``_acting``) to enter and leave an execution, for ``NoExecution`` and
      to close a context after a token misuse; ``tests/unit/test_layers.py``
      lists the modules that may.
    - Expiry is checked where work starts (:meth:`Acting.pinned_for`), at
      the start of every SQLAlchemy transaction, when a task whose execution
      ended reads its caller and at every AI call
      (:meth:`Acting.refuse_expired`), never by :func:`current_identity`.
    - Every explicit switch (``executor.run(..., identity=X)``,
      ``invoker.for_identity(X)``, ``ExecutionScope(X)``, a transport
      replacing the caller) and every refusal past a deadline is reported
      here: an ``identity.caller_switched`` or ``identity.delegation_expired`` warning, and
      span attributes for a switch.
    - This module imports nothing from the execution, the engine, the
      container or the use cases.
"""

from __future__ import annotations

import logging
import math
import time
from contextvars import ContextVar, Token

import msgspec

from loom.core.config.errors import ConfigError
from loom.core.errors import DelegationExpired, IdentityFixed
from loom.core.identity.identity import ANONYMOUS, Identity
from loom.core.identity.wire import NO_DELEGATION, Delegation
from loom.core.log_names import LogEvent, LogField, LogKey
from loom.core.model.types import PositiveSeconds

_log = logging.getLogger(__name__)


class IdentityEvent(LogEvent):
    """What the acting identity logs."""

    DELEGATION_EXPIRED = "identity.delegation_expired"
    CALLER_SWITCHED = "identity.caller_switched"


class IdentityKey(LogKey):
    """The fields of an identity record, and the span attributes of a switch."""

    SWITCHED_FROM = "loom.identity.switched_from"
    SWITCHED_TO = "loom.identity.switched_to"
    ROLES_FROM = "loom.identity.roles_from"
    ROLES_TO = "loom.identity.roles_to"
    ATTRIBUTES_CHANGED = "loom.identity.attributes_changed"


class Acting(msgspec.Struct, frozen=True, gc=False):
    """The value of the acting identity: who acts, until when, and for which work.

    Attributes:
        effective: The caller, or ``ANONYMOUS`` for no caller.
        caller: The acting identity, ``None`` for no caller.
        deadline: The end of the caller's delegation (a :func:`time.time`
            value): finite while a caller acts, ``inf`` with none.
        work: The work that pinned the identity, opaque here; ``None`` at
            transport level.
    """

    effective: Identity
    caller: Identity | None
    deadline: float
    work: object | None

    @classmethod
    def delegated(
        cls, caller: Identity | None, *, until: float | None = None, work: object | None = None
    ) -> Acting:
        """Return *caller* acting within a delegation: the one deadline rule.

        Args:
            caller: The caller; ``None`` for no caller, which has no deadline.
            until: The deadline of the delegation it resumes, clamped to
                ``now + identity.max_delegation_seconds``; none starts one of
                that length.
            work: The work it acts for; ``None`` at transport level.

        Returns:
            The acting identity.
        """
        if caller is None:
            return cls(ANONYMOUS, None, math.inf, work)
        window_end = time.time() + _max_delegation_seconds
        deadline = window_end if until is None else min(until, window_end)
        return cls(caller, caller, deadline, work)

    @classmethod
    def current(cls) -> Acting:
        """Return the acting identity of the current context."""
        return _acting.get()

    def pinned_for(self, identity: Identity | None, work: object, what: str) -> Acting:
        """Return *identity* pinned for *work* over this caller and deadline: the rule of a switch.

        - The same subject as the caller acting keeps its deadline, and is
          refused once it has passed; other roles or attributes are a switch.
        - Another subject is a switch with a new delegation.
        - No caller has no deadline.

        A switch is reported when someone was acting or this is work.

        Args:
            identity: The identity the work acts as; ``None`` for no caller.
            work: The work that pins it; opaque here.
            what: What runs, for the switch report (the use case's name).

        Returns:
            The value the work installs.

        Raises:
            DelegationExpired: Acting as the subject acting past its deadline.
        """
        caller = self.caller
        if identity is None or caller is None or identity.subject != caller.subject:
            pinned = Acting.delegated(identity, work=work)
        else:
            in_time = time.time() <= self.deadline
            if not in_time:
                self.refuse_expired()
            pinned = Acting(identity, identity, self.deadline, work)
        if identity is not caller and (caller is not None or self.work is not None):
            self.report_switch(identity, what)
        return pinned

    def resumed(self, delegation: Delegation) -> Delegation | None:
        """Return what queued work installs here to resume *delegation*, or ``None`` for nothing.

        - No caller: none installed, or no caller over an authenticated one.
        - Another caller than this one: *delegation* as is.
        - This caller, inside work: nothing, the work's delegation governing.
        - This caller, outside work: *delegation* ending at this caller's
          deadline when that comes first.

        Args:
            delegation: The dispatcher's caller and deadline.

        Returns:
            The delegation to install, or ``None``.

        Raises:
            DelegationExpired: Inside work acting as the dispatcher, past the
                captured deadline.
        """
        caller, until = delegation
        if caller is None:
            return NO_DELEGATION if self.effective.is_authenticated else None
        if caller != self.caller:
            return delegation
        if self.work is not None:
            if until is not None and until < self.deadline:
                Acting(caller, caller, until, self.work).refuse_expired()
            return None
        return Delegation.from_caller(
            caller, self.deadline if until is None else min(until, self.deadline)
        )

    def refuse_expired(self) -> None:
        """Refuse acting as this caller past its deadline, logging ``identity.delegation_expired``.

        Raises:
            DelegationExpired: When a caller acts past its deadline.
        """
        in_time = time.time() <= self.deadline
        if not in_time:
            error = DelegationExpired()
            _log.warning(IdentityEvent.DELEGATION_EXPIRED)
            raise error

    def report_switch(self, identity: Identity | None, what: str) -> None:
        """Log that *what* acts as *identity* instead of this caller, and record it on the span.

        Nothing is reported when *identity* equals this caller. Subjects and
        roles only, never an identity's attributes or credentials:
        ``attributes_changed`` flags two callers whose attributes differ.

        Args:
            identity: The identity acting from it on; ``None`` for no caller.
            what: What runs as *identity*: the use case, ``ExecutionScope`` or
                ``transport``.
        """
        previous = self.caller
        if identity == previous:
            return
        from loom.core.observability.span import annotate_current_span

        from_subject = getattr(previous, "subject", None)
        to_subject = getattr(identity, "subject", None)
        _log.warning(
            IdentityEvent.CALLER_SWITCHED,
            extra={
                IdentityKey.SWITCHED_FROM: from_subject,
                IdentityKey.SWITCHED_TO: to_subject,
                IdentityKey.ROLES_FROM: getattr(previous, "roles", None),
                IdentityKey.ROLES_TO: getattr(identity, "roles", None),
                IdentityKey.ATTRIBUTES_CHANGED: previous is not None
                and identity is not None
                and previous.attributes != identity.attributes,
                LogField.USE_CASE: what,
            },
        )
        annotate_current_span(
            {
                IdentityKey.SWITCHED_FROM: from_subject or "",
                IdentityKey.SWITCHED_TO: to_subject or "",
            }
        )


DEFAULT_MAX_DELEGATION_SECONDS = 3600.0
"""Default of ``identity.max_delegation_seconds``: one hour of delegated work per request."""

_NO_ONE = Acting(ANONYMOUS, None, math.inf, None)
_acting: ContextVar[Acting] = ContextVar("loom_acting_identity", default=_NO_ONE)
_max_delegation_seconds = DEFAULT_MAX_DELEGATION_SECONDS


def configure_delegation(seconds: float) -> None:
    """Set ``identity.max_delegation_seconds`` for the process.

    Args:
        seconds: How long work a request causes may act as its caller.

    Raises:
        ConfigError: When *seconds* is not a finite number above zero.
    """
    global _max_delegation_seconds
    try:
        _max_delegation_seconds = msgspec.convert(seconds, PositiveSeconds)
    except msgspec.ValidationError as error:
        raise ConfigError(
            f"identity.max_delegation_seconds must be a finite number above zero, got {seconds}"
        ) from error


def current_identity() -> Identity:
    """Return the acting identity, or :data:`~loom.core.identity.identity.ANONYMOUS`.

    Never returns ``None``: with no caller it yields ``ANONYMOUS``, so
    consumers cannot accidentally treat "unknown" as "authorized".

    Returns:
        The acting identity, or the anonymous one.
    """
    return _acting.get().effective


def current_caller() -> Identity | None:
    """Return the acting identity, or ``None`` when no caller acts.

    ``None`` (no caller) differs from
    :data:`~loom.core.identity.identity.ANONYMOUS` (an explicit anonymous
    caller).

    Returns:
        The acting identity, or ``None``.
    """
    return _acting.get().caller


def set_identity(identity: Identity | None, *, until: float | None = None) -> Token[Acting]:
    """Install the caller a transport verified, for the current context.

    Replacing a different caller is a switch, logged
    (``identity.caller_switched``).

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
    token = _acting.set(Acting.delegated(identity, until=until))
    previous = token.old_value
    if previous is Token.MISSING:
        return token
    if previous.work is not None:
        _acting.reset(token)
        raise IdentityFixed()
    if previous.caller is not None and previous.caller is not identity:
        try:
            previous.report_switch(identity, "transport")
        except BaseException:
            _acting.reset(token)
            raise
    return token


def reset_identity(token: Token[Acting]) -> None:
    """Restore the identity active before the matching :func:`set_identity`.

    Args:
        token: Token returned by the corresponding :func:`set_identity` call.
    """
    _acting.reset(token)


def captured_delegation() -> Delegation:
    """Return the caller acting and its deadline, as work dispatched now captures them.

    A plain read. An unauthenticated caller (``ANONYMOUS``) is captured as no
    caller, the job wire's policy, in every backend.

    Returns:
        The authenticated caller and its deadline, or no caller and no
        deadline.
    """
    acting = _acting.get()
    return Delegation.from_caller(acting.caller, acting.deadline)
