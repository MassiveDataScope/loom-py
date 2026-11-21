"""The names of what loom and an application log: one closed vocabulary per area."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Final

EVENT_FORM: Final = re.compile(
    r"[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)*\.[a-z][a-z0-9]*(?:_[a-z0-9]+)+"
)
"""``<area>.<subject>_<fact>``, lowercase: ``job.delivery_abandoned``."""


class LogEvent(StrEnum):
    """Base of an area's log events.

    A record's message is the event, never a sentence with values in it: the
    values travel in the record's fields (:class:`~loom.core.logger.LogField`),
    so a collector can filter on both. Every member of a subclass is named
    ``<area>.<subject>_<fact>`` in lowercase; an architecture test enforces it.

    Example::

        class InvoiceEvent(LogEvent):
            ISSUED = "billing.invoice_issued"

        log.info(InvoiceEvent.ISSUED, extra={"invoice.id": invoice_id})
    """

    @property
    def is_well_formed(self) -> bool:
        """Whether the value follows ``<area>.<subject>_<fact>``."""
        return EVENT_FORM.fullmatch(self.value) is not None
