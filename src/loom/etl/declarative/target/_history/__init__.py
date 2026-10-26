"""SCD Type 2 (historify) target — builder, spec, enums, report, and errors.

Public API — import from :mod:`loom.etl.declarative.target` or :mod:`loom.etl`.
"""

from loom.etl.declarative.target._history._builder import IntoHistory
from loom.etl.declarative.target._history._enums import (
    DeletePolicy,
    HistorifyInputMode,
    HistoryDateType,
)
from loom.etl.declarative.target._history._errors import (
    HistorifyDateCollisionError,
    HistorifyDeleteMarkerError,
    HistorifyKeyConflictError,
    HistorifyTemporalConflictError,
)
from loom.etl.declarative.target._history._report import HistorifyRepairReport
from loom.etl.declarative.target._history._spec import HistorifySpec

__all__ = [
    "HistorifyInputMode",
    "DeletePolicy",
    "HistoryDateType",
    "HistorifySpec",
    "HistorifyRepairReport",
    "HistorifyKeyConflictError",
    "HistorifyDateCollisionError",
    "HistorifyDeleteMarkerError",
    "HistorifyTemporalConflictError",
    "IntoHistory",
]
