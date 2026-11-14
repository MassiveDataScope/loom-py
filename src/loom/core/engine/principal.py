"""Who one execution runs as when it is given no identity.

:class:`~loom.core.engine.executor.RuntimeExecutor` runs an execution as the
identity it is given, else as the caller of the execution enclosing it
(:func:`inherited`, NS2), else as no one. A task that outlived the execution
it was started in runs as at top level (:func:`top_level`): nothing of the
departed caller survives into the execution it starts.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from loom.core.engine.post_commit import bind_channel, reset_channel
from loom.core.errors import NestedIdentityMismatch
from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS, Identity, current_identity, reset_identity, set_identity
from loom.core.uow.scope import unscoped


def inherited(use_case: type[Any]) -> Identity | None:
    """The caller of the execution enclosing *use_case*'s, if any (NS2).

    The ambient identity must still be that caller, or
    :class:`~loom.core.errors.NestedIdentityMismatch` is raised: code changed
    it during the enclosing execution, or opened that execution with
    ``ExecutionScope`` / ``enter`` without ``set_identity`` of the same
    identity. The message names no identity, as the other refusals do.
    """
    enclosing = current_execution()
    identity = None if enclosing is None else enclosing.identity
    if identity is None:
        return None
    ambient = current_identity()
    if ambient is not identity and ambient != identity:  # `is`: the parent installed it
        raise NestedIdentityMismatch(
            f"{use_case.__qualname__} was given no identity, and the ambient identity "
            "differs from the enclosing execution's: it was changed during that execution, or "
            "the execution was opened (ExecutionScope, enter) without set_identity; pass "
            "identity=... explicitly"
        )
    return identity


@contextmanager
def top_level(identity: Identity | None) -> Iterator[None]:
    """Hide what a departed execution left in this task, for one execution started from it.

    No invocation scope, no post-commit channel, and the ambient identity
    *identity*, else :data:`~loom.core.identity.ANONYMOUS`: nothing of the
    departed caller survives. Its execution context needs no hiding: an
    ended one is no execution (:func:`~loom.core.execution.current_execution`).
    """
    identity_token = set_identity(ANONYMOUS if identity is None else identity)
    channel_token = bind_channel(None)
    try:
        with unscoped():
            yield
    finally:
        reset_channel(channel_token)
        reset_identity(identity_token)
