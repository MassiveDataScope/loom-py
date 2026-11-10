"""Audit errors map to safe server responses."""

from http import HTTPStatus
from typing import cast

import pytest

from loom.core.audit.errors import AuditReadFailed, AuditSubjectsInvalid
from loom.core.errors import SystemError
from loom.core.errors.codes import ErrorCode
from loom.rest.errors import HttpErrorMapper


@pytest.mark.parametrize(
    ("error_type", "code"),
    [
        (AuditSubjectsInvalid, ErrorCode.AUDIT_SUBJECTS_INVALID),
        (AuditReadFailed, ErrorCode.AUDIT_READ_FAILED),
    ],
)
def test_audit_errors_are_system_errors_with_safe_http_response(
    error_type: type[AuditSubjectsInvalid] | type[AuditReadFailed], code: ErrorCode
) -> None:
    error = error_type()
    assert isinstance(error, SystemError)
    assert error.code is code
    assert HttpErrorMapper._STATUS[code] is HTTPStatus.INTERNAL_SERVER_ERROR
    http = HttpErrorMapper().to_http(error)
    assert http.status_code == 500
    detail = cast(dict[str, object], http.detail)
    assert detail["code"] == code
    message = detail["message"]
    assert isinstance(message, str)
    assert "SQL" not in message
