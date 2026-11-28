"""What the REST layer logs and the fields of each record."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey, ValueKind


class RestEvent(LogEvent):
    """The events the REST layer logs."""

    COLUMNS_ALL_EXPOSED = "loom.rest.columns_all_exposed"
    TOKEN_ISSUED = "loom.rest.token_issued"
    TOKEN_SIGNING_FAILED = "loom.rest.token_signing_failed"
    TOKEN_INVALID = "loom.rest.token_invalid"
    TOKEN_TYPE_REFUSED = "loom.rest.token_type_refused"
    TOKEN_KEY_UNKNOWN = "loom.rest.token_key_unknown"
    TOKEN_ROLES_CLAIM_MISSING = "loom.rest.token_roles_claim_missing"
    AUTHENTICATION_UNAVAILABLE = "loom.rest.authentication_unavailable"
    AUTHENTICATION_REFUSED = "loom.rest.authentication_refused"
    AUTHORIZATION_DENIED = "loom.rest.authorization_denied"
    REQUEST_UNHANDLED = "loom.rest.request_unhandled"
    HTTP_ERROR_SERVED = "loom.rest.http_error_served"
    HTTP_ERROR_MISDECLARED = "loom.rest.http_error_misdeclared"
    SQL_ENDPOINT_MOUNTED = "loom.rest.sql_endpoint_mounted"
    SQL_ENDPOINT_FAILED = "loom.rest.sql_endpoint_failed"
    SQL_ENDPOINT_UNHANDLED = "loom.rest.sql_endpoint_unhandled"


class RestKey(LogKey):
    """The fields of a REST record."""

    ERROR_CLASS = "loom.rest.error_class"
    ROLES_BOUND = "loom.rest.roles_bound", ValueKind.BOOLEAN
