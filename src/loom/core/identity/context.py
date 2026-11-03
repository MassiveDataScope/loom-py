"""Active caller-identity context guard.

Provides a ``contextvars.ContextVar`` that propagates the verified caller
across the async execution stack — from the transport that authenticated the
request down to the layer that takes an authorization decision.

Rules:
    - Only the transport layer (HTTP middleware, Celery worker, test harness)
      and the :class:`~loom.core.engine.executor.RuntimeExecutor` call
      :func:`set_identity` / :func:`reset_identity`; the executor installs
      the identity it was given for the length of one execution.
    - Any layer may call :func:`current_identity` without introducing upward
      coupling.
    - The token returned by :func:`set_identity` **must** be passed to
      :func:`reset_identity` in a ``finally`` block: without it, a reused
      worker task inherits the previous caller's identity.
"""

from __future__ import annotations

from contextvars import ContextVar, Token

from loom.core.identity.identity import ANONYMOUS, Identity

_identity: ContextVar[Identity] = ContextVar("_identity", default=ANONYMOUS)


def current_identity() -> Identity:
    """Return the identity active in the current async context.

    Never returns ``None``: contexts with no authenticated caller yield
    :data:`~loom.core.identity.identity.ANONYMOUS`, so consumers cannot
    accidentally treat "unknown" as "authorized".

    Returns:
        The active identity, or the anonymous one.
    """
    return _identity.get()


def set_identity(identity: Identity) -> Token[Identity]:
    """Install *identity* for the current async context.

    Args:
        identity: Verified identity produced by an authentication mechanism.

    Returns:
        A :class:`~contextvars.Token` that must be passed to
        :func:`reset_identity` in a ``finally`` block.

    Example::

        token = set_identity(identity)
        try:
            await handle_request()
        finally:
            reset_identity(token)
    """
    return _identity.set(identity)


def reset_identity(token: Token[Identity]) -> None:
    """Restore the identity active before the matching :func:`set_identity`.

    Args:
        token: Token returned by the corresponding :func:`set_identity` call.
    """
    _identity.reset(token)
