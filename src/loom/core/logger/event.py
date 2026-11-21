"""The names of what loom and an application log: one closed vocabulary per area."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Final

EVENT_FORM: Final = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+")
"""Dotted lowercase words, the first one the area: ``job.delivery_abandoned``."""


class LogEvent(StrEnum):
    """Base of an area's log events.

    A record's message is the event, never a sentence with values in it: the
    values travel in the record's fields (:class:`~loom.core.logger.LogField`),
    so a collector can filter on both. A subclass declares the prefix of its area
    once, as a class keyword, and every member value starts with it and a dot;
    an architecture test enforces it. A prefix is a public name: moving the
    package that owns the events does not rename them.

    Example::

        class InvoiceLogEvent(LogEvent, prefix="billing"):
            ISSUED = "billing.invoice_issued"

        log.info(InvoiceLogEvent.ISSUED, extra={"invoice.id": invoice_id})
    """

    prefix: str

    def __init_subclass__(cls, *, prefix: str, **kwargs: object) -> None:
        """Record the area prefix the members of the subclass start with.

        Raises:
            ValueError: When *prefix* is not lowercase words joined by ``_`` or ``.``.
        """
        super().__init_subclass__(**kwargs)
        if EVENT_FORM.fullmatch(f"{prefix}.x") is None:
            raise ValueError(f"{cls.__name__}: {prefix!r} is not a lowercase event prefix")
        cls.prefix = prefix

    @property
    def is_well_formed(self) -> bool:
        """Whether the value is dotted lowercase words and starts with the class prefix."""
        in_area = self.value.startswith(f"{self.prefix}.")
        return in_area and EVENT_FORM.fullmatch(self.value) is not None
