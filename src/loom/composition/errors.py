"""Composition error types.

``CompositionErrorCode``
    Catalogue of the start-up failures the composition reports about the
    caller's DI modules. Each member carries the template of its issue, and
    ``CompositionErrorCode.X.issue(...)`` builds it.

``ReservedBindingError``
    A caller's DI module bound a key the framework owns. The composition
    raises it right after the caller's modules run, before any repository or
    framework service is registered.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import unique

from loom.core.bootstrap import BootstrapError
from loom.core.di.container import BindingKey
from loom.core.errors.codes import CompileIssue, IssueCode, Template


@unique
class CompositionErrorCode(IssueCode):
    """Catalogue of the composition failures and the issue each renders."""

    RESERVED_BINDING = (
        "RESERVED_BINDING",
        Template(
            "DI module {component} binds {key}, which the framework registers itself. "
            "Remove the binding from the module; a module may override JobService and "
            "repositories (repository_for, DefaultRepositoryBuilder), not this key.",
            field="{key}",
        ),
    )


class ReservedBindingError(BootstrapError):
    """Raised at start-up when a caller's DI module binds a framework-owned key.

    A :class:`~loom.core.bootstrap.BootstrapError`, so a host catching a
    failed bootstrap catches this one too, and the bootstrap passes it on
    unwrapped.

    Attributes:
        issue: The typed issue: code, component (the module), field (the key)
            and message.
        key: The reserved key the module bound.

    Example::

        raise ReservedBindingError.for_key(register_sql, SqlQueryService)
    """

    def __init__(self, issue: CompileIssue[CompositionErrorCode], key: BindingKey) -> None:
        super().__init__(issue.message)
        self.issue = issue
        self.key = key

    @classmethod
    def for_key(cls, module: Callable[..., object], key: BindingKey) -> ReservedBindingError:
        """Return the error for the caller's *module* binding *key*."""
        component = getattr(module, "__qualname__", None) or repr(module)
        issue = CompositionErrorCode.RESERVED_BINDING.issue(component=component, key=key)
        return cls(issue, key)


__all__ = ["CompositionErrorCode", "ReservedBindingError"]
