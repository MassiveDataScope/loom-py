"""Use-case wiring error types.

``UseCaseErrorCode``
    Catalogue of the start-up failures :meth:`UseCaseFactory.verify
    <loom.core.use_case.factory.UseCaseFactory.verify>` reports. Each member
    carries the template of its issue, and ``UseCaseErrorCode.X.issue(...)``
    builds it.

``UnservedModelError``
    A use case or job reads a model no repository is registered for: through
    ``self.main_repo``, or through ``LoadById``, ``Load`` or ``Exists``. Every
    entry point raises it before serving, from ``verify``, instead of failing
    the first request or task.
"""

from __future__ import annotations

from enum import unique

from loom.core.di.container import ResolutionError
from loom.core.errors.codes import CompileIssue, IssueCode, Template


@unique
class UseCaseErrorCode(IssueCode):
    """Catalogue of the use-case wiring failures and the issue each renders."""

    UNSERVED_MODEL = (
        "UNSERVED_MODEL",
        Template(
            "{component} injects {parameter}: repository for {model}, which is not "
            "registered in the container. Add {model} to MODELS in your manifest module, "
            "or include its module in app.discovery, so its repository is registered.",
            field="{parameter}",
        ),
    )


class UnservedModelError(ResolutionError):
    """Raised at start-up when a use case or job reads a model no repository serves.

    A :class:`~loom.core.di.container.ResolutionError`, so a host catching a
    missing binding catches this one too.

    Attributes:
        issue: The typed issue: code, component (the use case or job), field
            (the parameter) and message.
        model: The model nothing serves.

    Example::

        raise UnservedModelError.for_parameter(ListOrders, "main_repo", Order)
    """

    def __init__(self, issue: CompileIssue[UseCaseErrorCode], model: type[object]) -> None:
        super().__init__(issue.message)
        self.issue = issue
        self.model = model

    @classmethod
    def for_parameter(
        cls, component: type[object], parameter: str, model: type[object]
    ) -> UnservedModelError:
        """Return the error for *component*'s *parameter* reading *model*."""
        issue = UseCaseErrorCode.UNSERVED_MODEL.issue(
            component=component, parameter=parameter, model=model
        )
        return cls(issue, model)


__all__ = ["UnservedModelError", "UseCaseErrorCode"]
