"""The field keys of the ETL runtime."""

from __future__ import annotations

from loom.core.logger import LogKey


class EtlLogField(LogKey):
    """The field keys of a pipeline, process and step run."""

    RUN_ID = "loom.etl.run_id"
    LAST_ATTEMPT = "loom.etl.last_attempt"
    PROCESS_RUN_ID = "loom.etl.process_run_id"
    STEP_RUN_ID = "loom.etl.step_run_id"
    SOURCES = "loom.etl.sources"
    TARGET = "loom.etl.target"
    STREAMING = "loom.etl.streaming"
