"""What the Mongo store logs and the fields of each record."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey


class MongoEvent(LogEvent):
    """The events the Mongo store logs."""

    UNIT_OF_WORK_CHOSEN = "mongo.unit_of_work_chosen"
    READINESS_PROBE_FAILED = "mongo.readiness_probe_failed"
    BULK_UNDO_FAILED = "mongo.bulk_undo_failed"


class MongoKey(LogKey):
    """The fields of a Mongo store record."""

    TRANSACTIONS = "loom.mongo.transactions"
    DOCUMENT_IDS = "loom.mongo.document_ids"
