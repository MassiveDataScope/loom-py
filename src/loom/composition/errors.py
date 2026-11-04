"""Composition error types.

``CompositionErrorCode``
    Catalogue of the start-up failures the composition reports about the
    caller's DI modules. Each member carries the template of its issue, and
    ``CompositionErrorCode.X.issue(...)`` builds it.

``ReservedBindingError``
    A caller's DI module bound a key the framework owns. The composition
    raises it right after the caller's modules run, before any repository or
    framework service is registered.

``JobBackendError``
    A job backend ``app.job_service`` or ``celery.queues`` needs cannot be built.

``CompositionStateError``
    An adapter or an :class:`~loom.composition.ApplicationHost` was used out
    of its lifecycle: an adapter composing a second application or bound
    before its recipe, a host started twice or stopped while opening.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import unique

from loom.core.bootstrap import BootstrapError
from loom.core.config.errors import ConfigError
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
    APPLICATION_ALREADY_OPEN = (
        "APPLICATION_ALREADY_OPEN",
        Template("Application is already open; close it before opening it again", component=""),
    )
    ADAPTER_ALREADY_COMPOSED = (
        "ADAPTER_ALREADY_COMPOSED",
        Template("one adapter instance composes one application; create another adapter"),
    )
    ADAPTER_RECIPE_MISSING = (
        "ADAPTER_RECIPE_MISSING",
        Template("{component}.bind needs the recipe it declared; call recipe first"),
    )
    HOST_ALREADY_STARTED = (
        "HOST_ALREADY_STARTED",
        Template(
            "ApplicationHost opens its application once; create another host",
            component="ApplicationHost",
        ),
    )
    HOST_STOPPED_WHILE_OPENING = (
        "HOST_STOPPED_WHILE_OPENING",
        Template(
            "ApplicationHost was stopped while opening; the application is closed",
            component="ApplicationHost",
        ),
    )


class ApplicationAlreadyOpenError(RuntimeError):
    """Raised when an application's resources are opened a second time."""

    def __init__(self) -> None:
        self.issue = CompositionErrorCode.APPLICATION_ALREADY_OPEN.issue()
        super().__init__(self.issue.message)


class CompositionStateError(RuntimeError):
    """Raised when an adapter or a host is used out of its lifecycle.

    A :class:`RuntimeError`, so a caller catching the lifecycle misuse as
    one keeps catching it.

    Attributes:
        issue: The typed issue: code, component (the adapter or the host) and
            message.

    Example::

        raise CompositionStateError(
            CompositionErrorCode.ADAPTER_RECIPE_MISSING.issue(component="CeleryAdapter")
        )
    """

    def __init__(self, issue: CompileIssue[CompositionErrorCode]) -> None:
        super().__init__(issue.message)
        self.issue = issue


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


class JobBackendError(ConfigError):
    """Raised at start-up when a job backend the configuration needs cannot be built."""


__all__ = [
    "ApplicationAlreadyOpenError",
    "CompositionErrorCode",
    "CompositionStateError",
    "JobBackendError",
    "ReservedBindingError",
]
