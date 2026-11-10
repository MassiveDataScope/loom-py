"""Safe audit read failures."""

from loom.core.errors import SystemError
from loom.core.errors.codes import ErrorCode


class AuditSubjectsInvalid(SystemError):
    """The subject resolver returned an invalid value."""

    def __init__(self) -> None:
        super().__init__("Audit subjects are invalid", code=ErrorCode.AUDIT_SUBJECTS_INVALID)


class AuditReadFailed(SystemError):
    """The audit log could not be read."""

    def __init__(self) -> None:
        super().__init__("Audit log read failed", code=ErrorCode.AUDIT_READ_FAILED)
