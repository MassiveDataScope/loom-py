"""Value rendering on handlers: ``makeRecord`` applies ``extra`` after the record factory.

So the ``extra`` values are rendered by :class:`LogFilter`, a filter loom puts
on the handlers it owns and on the Lambda runtime's handler.
"""

from __future__ import annotations

import logging
from typing import Final

from loom.core.fields import render_value
from loom.core.logger.record import STAMP, RecordStamp

STANDARD_ATTRIBUTES: Final = frozenset(logging.makeLogRecord({}).__dict__) | {
    "message",
    "asctime",
}
"""The attributes the standard library gives every record; any other one is a field."""


class LogFilter(logging.Filter):
    """Render the field values of a record and write its stamp as fields; never drops one.

    A field whose value is ``None`` is not known, and is not written, so a
    stamped field such as ``trace_id`` fills it. The filter is total: it
    raises for no value. The stamp the record factory left is its mark: the
    first handler's filter renders the values, writes the stamp as fields and
    removes it, and every later one lets the record through unchanged. A record
    no :class:`~loom.core.logger.record.LoomRecordFactory` created carries no
    stamp and passes as it is.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Render *record*'s values in place and let it through."""
        fields = record.__dict__
        stamp = fields.pop(STAMP, None)
        if not isinstance(stamp, RecordStamp):
            return True
        unknown: list[str] = []
        for key, value in fields.items():
            if key in STANDARD_ATTRIBUTES:
                continue
            if value is None:
                unknown.append(key)
            else:
                fields[key] = render_value(value)
        for key in unknown:
            del fields[key]
        stamp.write_into(fields, record)
        return True
