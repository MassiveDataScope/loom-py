"""Find the record that describes a failure a handoff replaced with a reference.

A handoff raises a :class:`~loom.core.logger.failure.FailureReference`, which
holds no original exception and no message. The failure's details are in
loom's log: its type, its code, the fields it declares and its stack. A test
that sees the reference reads them from the record that carries the same
trace and type.

Example::

    from loom.testing.failures import logged_failure

    def test_a_failed_job_is_logged(caplog, worker) -> None:
        with pytest.raises(FailureReference) as raised:
            worker.run(FailingJob)

        record = logged_failure(caplog.records, raised.value)
        assert record.__dict__["error.code"] == "conflict"
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from loom.core.logger.failure import FailureReference
from loom.core.logger.record import STAMP, RecordStamp
from loom.core.logger.vocabulary import LogField


def logged_failure(
    records: Iterable[logging.LogRecord], handed_off: FailureReference
) -> logging.LogRecord:
    """The first record that describes the failure *handed_off* replaced.

    It is the first record made by loom's record factory whose exception has
    the reference's type and whose trace is the reference's: the innermost
    one, which wrote the stack of an unexpected failure. It reads the record
    as a loom handler does, before or after loom's filter ran on it.

    Args:
        records: The captured records, in the order they were made
            (``caplog.records``).
        handed_off: The reference the handoff raised.

    Raises:
        LookupError: When no record describes it, for instance because loom's
            record factory was not installed.
    """
    for record in records:
        fields = _fields(record)
        if (
            fields.get(LogField.TRACE_ID.value) == handed_off.trace_id
            and fields.get(LogField.EXCEPTION_TYPE.value) == handed_off.exception_type
        ):
            return record
    raise LookupError(
        f"no record describes {handed_off.exception_type} in trace {handed_off.trace_id}"
    )


def _fields(record: logging.LogRecord) -> dict[str, Any]:
    """The fields of *record*, whether a loom filter already wrote its stamp or not."""
    fields = dict(record.__dict__)
    stamp = fields.get(STAMP)
    if isinstance(stamp, RecordStamp):
        stamp.write_into(fields, record)
    return fields


__all__ = ["logged_failure"]
