"""What the REST layer logs and the fields of each record."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey


class RestEvent(LogEvent):
    """The events the REST layer logs."""

    COLUMNS_ALL_EXPOSED = "rest.columns_all_exposed"
    TOKEN_ISSUED = "rest.token_issued"
    TOKEN_SIGNING_FAILED = "rest.token_signing_failed"
    TOKEN_INVALID = "rest.token_invalid"
    TOKEN_TYPE_REFUSED = "rest.token_type_refused"
    TOKEN_KEY_UNKNOWN = "rest.token_key_unknown"
    TOKEN_ROLES_CLAIM_MISSING = "rest.token_roles_claim_missing"
    AUTHENTICATION_UNAVAILABLE = "rest.authentication_unavailable"
    AUTHENTICATION_REFUSED = "rest.authentication_refused"
    AUTHORIZATION_DENIED = "rest.authorization_denied"
    REQUEST_UNHANDLED = "rest.request_unhandled"
    HTTP_ERROR_SERVED = "rest.http_error_served"
    HTTP_ERROR_MISDECLARED = "rest.http_error_misdeclared"
    SQL_ENDPOINT_MOUNTED_SHARED_ROLE = "rest.sql_endpoint_mounted_shared_role"
    SQL_ENDPOINT_MOUNTED_ROLES_BOUND = "rest.sql_endpoint_mounted_roles_bound"
    SQL_ENDPOINT_FAILED = "rest.sql_endpoint_failed"
    SQL_ENDPOINT_UNHANDLED = "rest.sql_endpoint_unhandled"


class RestKey(LogKey):
    """The fields of a REST record."""

    ROUTE = "loom.rest.route"
    SUBJECT = "loom.rest.subject"
    CONNECTION = "loom.rest.connection"
    ERROR_CLASS = "loom.rest.error_class"
