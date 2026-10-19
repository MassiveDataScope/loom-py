"""Machine-readable error codes shared across the framework.

Two kinds of catalogue live here. ``ErrorCode`` holds the run-time codes a
transport maps to a status. ``IssueCode`` is the base of each pillar's
compile-issue catalogue: every member carries the :class:`Template` that
renders its :class:`CompileIssue`, so a code and its message are declared once.

``ErrorCode`` is a ``StrEnum`` so values are plain strings at runtime —
existing code that compares ``error.code == "not_found"`` continues to work
while new code can use the enum for typo-proof, IDE-navigable references.

Example::

    from loom.core.errors.codes import ErrorCode

    if error.code == ErrorCode.NOT_FOUND:
        ...
"""

from __future__ import annotations

from collections.abc import Iterable, Set
from enum import StrEnum
from string import Formatter
from typing import Generic, NamedTuple, Self, TypeVar

import msgspec


class ErrorCode(StrEnum):
    """Canonical error discriminators used by :class:`~loom.core.errors.LoomError` subclasses.

    The REST adapter maps these to HTTP status codes via
    :class:`~loom.rest.errors.HttpErrorMapper`.
    """

    NOT_FOUND = "not_found"
    UNAUTHENTICATED = "unauthenticated"
    INSUFFICIENT_AUTHENTICATION = "insufficient_authentication"
    FORBIDDEN = "forbidden"
    CONFLICT = "conflict"
    RULE_VIOLATION = "rule_violation"
    RULE_VIOLATIONS = "rule_violations"
    BOUNDARY_VALIDATION = "boundary_validation"
    QUERY_VALIDATION = "query_validation"
    UNSUPPORTED_FORMAT = "unsupported_format"
    UNSUPPORTED_QUERY = "unsupported_query"
    RATE_LIMITED = "rate_limited"
    SYSTEM_ERROR = "system_error"
    SERVICE_UNAVAILABLE = "service_unavailable"
    POST_COMMIT_FAILURE = "post_commit_failure"


class Template(NamedTuple):
    """How an :class:`IssueCode` renders its issue.

    Each part is a :meth:`str.format` template over the parameters passed to
    :meth:`IssueCode.issue`; a part without placeholders is a constant.

    Args:
        text: The issue message.
        field: The configuration or artifact field the issue points at, if any.
        component: The component the issue is attributed to.
    """

    text: str
    field: str | None = None
    component: str = "{component}"


CodeT = TypeVar("CodeT", bound="IssueCode")


class CompileIssue(msgspec.Struct, Generic[CodeT], frozen=True, kw_only=True):
    """One compile failure: its code, message, component and field.

    A plain ``msgspec.Struct``: ``LoomFrozenStruct`` lives in
    ``loom.core.model``, which imports this module.
    """

    code: CodeT
    message: str
    component: str = ""
    field: str | None = None


class IssueCode(StrEnum):
    """Base of a pillar's compile-issue catalogue.

    A member is declared as ``NAME = "NAME", Template(...)``. Pillar
    catalogues subclass this and add ``@enum.unique``.

    Attributes:
        template: How the member renders its issue.
        params: Parameter names :meth:`issue` requires, exactly.
    """

    template: Template
    params: frozenset[str]

    def __new__(cls, value: str, template: Template) -> Self:
        member = str.__new__(cls, value)
        member._value_ = value
        member.template = template
        parts = (template.text, template.component, template.field or "")
        member.params = frozenset(
            name for part in parts for _, name, _, _ in Formatter().parse(part) if name
        )
        return member

    def issue(self, **params: object) -> CompileIssue[Self]:
        """Build this code's issue from its template parameters.

        Raises:
            TypeError: If a parameter is missing or not one of :attr:`params`.
        """
        if params.keys() != self.params:
            raise TypeError(f"{self.value} takes {sorted(self.params)}, got {sorted(params)}")
        values = {name: _render(value) for name, value in params.items()}
        field = self.template.field
        return CompileIssue(
            code=self,
            message=self.template.text.format_map(values),
            component=self.template.component.format_map(values),
            field=None if field is None else field.format_map(values),
        )


def _render(value: object) -> str:
    """Render one parameter: a type by name, an iterable joined, ``"none"`` when empty."""
    if isinstance(value, str):
        return value
    if isinstance(value, type):
        return value.__qualname__
    if isinstance(value, Iterable):
        items = [_render(item) for item in value]
        if isinstance(value, Set):
            items.sort()
        return ", ".join(items) or "none"
    return str(value)
