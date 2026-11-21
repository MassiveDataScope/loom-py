"""Framework error bases: a loom error is built from its declared fields, never free text."""

from __future__ import annotations

import string
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any, ClassVar, Final

from loom.core.errors.codes import ErrorCode
from loom.core.errors.log_events import ErrorLogField
from loom.core.fields import LogKey, render_value

EXPECTED_CODES: Final = frozenset(
    {
        ErrorCode.NOT_FOUND,
        ErrorCode.RULE_VIOLATION,
        ErrorCode.RULE_VIOLATIONS,
        ErrorCode.UNAUTHENTICATED,
        ErrorCode.INSUFFICIENT_AUTHENTICATION,
        ErrorCode.FORBIDDEN,
        ErrorCode.BOUNDARY_VALIDATION,
        ErrorCode.QUERY_VALIDATION,
    }
)
"""The codes of an outcome the caller caused: a not-found, a rule violation, a denial, a
validation error. Its scope ends at INFO, with no stack."""

CLIENT_CODES: Final = frozenset(
    {
        ErrorCode.BOUNDARY_VALIDATION,
        ErrorCode.QUERY_VALIDATION,
        ErrorCode.RULE_VIOLATION,
        ErrorCode.RULE_VIOLATIONS,
        ErrorCode.UNSUPPORTED_QUERY,
        ErrorCode.UNSUPPORTED_FORMAT,
    }
)
"""The codes whose message is about the data the client sent (a 422, a client-caused 400): the
client reads it, as :attr:`LoomError.client_message`."""

ABSENT: Final = "<absent>"
"""How a template shows a declared field the error was built without."""

_RESERVED_NAMES: Final = frozenset({"code", "client_detail"})


@dataclass(frozen=True, slots=True)
class ClientViolation:
    """One violation a client reads in a 422 answer: the field, its code and its text.

    The text describes the data the client sent; it never enters ``str(error)``
    or the log, which carry the count and the codes.

    Attributes:
        field: The field of the request the violation is about.
        code: A stable code for the violation.
        text: What the client is told.
    """

    field: str
    code: str
    text: str


class LoomError(Exception):
    """Base class for all framework errors: a code, a template and declared fields.

    A subclass declares, on the class, its :attr:`code` (a member of a
    catalogue such as :class:`~loom.core.errors.codes.ErrorCode`), its
    :attr:`template` and :attr:`field_keys`, which maps each placeholder of the
    template, a Python identifier, to a vocabulary key. It is built as
    ``Cls(**fields)``: each value is rendered once, here, as any field value is
    (a ``Secret`` masked, a long string cut, an object its type name), and the
    template is rendered from the rendered values. So ``str(error)`` holds no
    free text and is safe anywhere. ``args`` is ``(code,)``, and every
    declared class rebuilds from no arguments or from its code alone, a
    missing field shown as :data:`ABSENT`: Celery's JSON rebuild calls
    ``Cls(*args)``.

    A class that serves several codes takes ``code=`` as an override. A class
    that declares no template is not migrated yet (spec 019, L2c): it is built
    with a positional message, kept as it is.

    An application error may declare the HTTP answer itself, on the class or
    on the instance, and still not import a web framework:

    - ``http_status``: a 4xx or 5xx status (an :class:`~http.HTTPStatus` or
      its int), or ``None`` (the default) to keep the status the error's
      ``code`` maps to. Any other value is a misdeclaration: the REST mapper
      logs it and answers 500 with the fixed message.
    - ``public_detail``: extra fields the HTTP detail gains. A key the mapper
      already set is a misdeclaration, answered the same way.

    What a client reads is :attr:`client_message` and :attr:`client_detail`;
    any other answer carries the error's code.

    Args:
        *rebuilt: The code alone, when Celery or pickle rebuilds a declared
            class; the free-text message of a class not migrated yet.
        code: The code, for a class that serves several.
        client_detail: The violations only the client reads.
        **fields: The declared fields, by name.

    Raises:
        ErrorDeclarationError: When a field is not declared, or a positional
            argument of a declared class is not one of its codes.

    Attributes:
        is_retryable: Whether running the work again could still succeed; a
            subclass a retry would repeat or make worse sets it ``False``.
        http_status: A 4xx or 5xx status (an ``HTTPStatus`` or its int), or
            ``None`` to keep the code table.

    Example::

        class InvoiceOverdue(DomainError):
            code = BillingCode.INVOICE_OVERDUE
            template = "invoice {invoice_id} is {days} days overdue"
            field_keys = {"invoice_id": BillingLogField.INVOICE_ID, "days": LogField.COUNT}

        raise InvoiceOverdue(invoice_id=42, days=3)
    """

    code: StrEnum = ErrorCode.SYSTEM_ERROR
    template: ClassVar[str] = ""
    field_keys: ClassVar[Mapping[str, LogKey]] = MappingProxyType({})
    is_retryable: bool = True
    http_status: int | None = None
    client_detail: tuple[ClientViolation, ...] = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Check that a declared template and its field map name the same fields.

        Raises:
            ErrorDeclarationError: When a placeholder has no key, a key has no
                placeholder, a placeholder is not a plain identifier or a field
                takes a reserved name.
        """
        super().__init_subclass__(**kwargs)
        if "template" not in cls.__dict__ and "field_keys" not in cls.__dict__:
            return
        reason = cls._declaration_fault()
        if reason is not None:
            raise ErrorDeclarationError(error_class=cls.__qualname__, reason=reason)

    @classmethod
    def _declaration_fault(cls) -> Misdeclaration | None:
        placeholders: set[str] = set()
        for _, name, spec, conversion in string.Formatter().parse(cls.template):
            if name is None:
                continue
            if spec or conversion or not name.isidentifier():
                return Misdeclaration.PLACEHOLDER_NOT_A_NAME
            placeholders.add(name)
        if placeholders != set(cls.field_keys):
            return Misdeclaration.FIELDS_DIFFER_FROM_TEMPLATE
        if placeholders & _RESERVED_NAMES:
            return Misdeclaration.RESERVED_FIELD_NAME
        return None

    def __init__(
        self,
        *rebuilt: object,
        code: StrEnum | None = None,
        client_detail: Sequence[ClientViolation] = (),
        **fields: object,
    ) -> None:
        if code is not None:
            self.code = code
        if client_detail:
            self.client_detail = tuple(client_detail)
        if not self.is_declared:
            self.message = str(rebuilt[0]) if rebuilt else type(self).__qualname__
            self._fields: dict[str, object] = {}
            super().__init__(self.message)
            return
        if rebuilt:
            self.code = self._rebuilt_code(rebuilt)
        undeclared = fields.keys() - self.field_keys.keys()
        if undeclared:
            raise ErrorDeclarationError(
                error_class=type(self).__qualname__, reason=Misdeclaration.UNDECLARED_FIELD
            )
        self._fields = {name: render_value(fields.get(name)) for name in self.field_keys}
        self.message = self.template.format_map(
            {name: ABSENT if value is None else value for name, value in self._fields.items()}
        )
        super().__init__(self.code)

    @property
    def is_declared(self) -> bool:
        """Whether the class declares a template, so it is built from fields only."""
        return bool(self.template) or bool(self.field_keys)

    def _rebuilt_code(self, rebuilt: tuple[object, ...]) -> StrEnum:
        value = rebuilt[0]
        if len(rebuilt) == 1 and isinstance(value, str):
            catalogue = type(self.code)
            if value in catalogue._value2member_map_:
                return catalogue(value)
        raise ErrorDeclarationError(
            error_class=type(self).__qualname__, reason=Misdeclaration.FREE_TEXT
        )

    def __str__(self) -> str:
        return self.message if self.is_declared else super().__str__()

    def __reduce__(self) -> tuple[Any, ...]:
        """Pickle the error without calling ``__init__``, so a subclass may take any arguments.

        Returns:
            The class and arguments to rebuild the error and the state to restore.
        """
        return (type(self).__new__, (type(self), *self.args), self.__getstate__())

    def __setstate__(self, state: Any) -> None:
        """Restore the attributes :meth:`__reduce__` saved, those in ``__dict__`` and in slots.

        Args:
            state: The attribute mapping, or that mapping and the slot mapping.
        """
        attributes, slots = state if isinstance(state, tuple) else (state, {})
        for name, value in {**attributes, **slots}.items():
            setattr(self, name, value)

    def log_fields(self) -> Mapping[LogKey, object]:
        """The declared fields under their vocabulary keys, rendered; an absent one is left out.

        A class not migrated yet may override it to declare fields by hand.
        """
        values = getattr(self, "_fields", {})
        return {
            key: values[name]
            for name, key in self.field_keys.items()
            if values.get(name) is not None
        }

    def public_detail(self) -> Mapping[str, Any]:
        """Return fields an HTTP error detail may add. Empty unless overridden."""
        return {}

    @property
    def client_message(self) -> str | None:
        """The text a client reads about this error, or ``None`` to answer with its code.

        Its message when its code is in :data:`CLIENT_CODES`: text about the
        data the client sent. A subclass overrides it to declare otherwise.
        """
        return self.message if self.code in CLIENT_CODES else None

    @property
    def expected(self) -> bool:
        """Whether this error is an outcome the caller caused, not a fault.

        Its code is in :data:`EXPECTED_CODES`: the one failure classification
        logging reads.
        """
        return self.code in EXPECTED_CODES


class DomainError(LoomError):
    """Base class for all business-logic errors.

    Raised inside UseCase execution. Transport adapters catch and convert
    these to their own error representations (a ``4xx``). Running the work
    again cannot fix the input or the rule it broke, so no transport retries
    one: a job raising it is consumed, not requeued.
    """

    is_retryable: bool = False


class SystemError(LoomError):
    """Base class for infrastructure and unexpected errors.

    Raised when a repository, external service, or internal subsystem
    fails. Transport adapters may retry on SystemError and dead-letter
    on DomainError.

    """


class Misdeclaration(StrEnum):
    """How an error class or its construction breaks the constructor's rules."""

    PLACEHOLDER_NOT_A_NAME = "placeholder_not_a_name"
    FIELDS_DIFFER_FROM_TEMPLATE = "fields_differ_from_template"
    RESERVED_FIELD_NAME = "reserved_field_name"
    UNDECLARED_FIELD = "undeclared_field"
    FREE_TEXT = "free_text"


class ErrorDeclarationError(SystemError):
    """Raised when an error class is declared, or built, outside the constructor's rules.

    A programming error: a template whose placeholders differ from its field
    map, a field that is not declared, or free text passed to a declared class.
    """

    is_retryable = False
    template = "{error_class} breaks the error constructor: {reason}"
    field_keys = MappingProxyType(
        {"error_class": ErrorLogField.ERROR_CLASS, "reason": ErrorLogField.MISDECLARATION}
    )
