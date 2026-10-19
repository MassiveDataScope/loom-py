"""ETL compilation error types.

``ETLErrorCode``
    Machine-readable catalogue of every validation failure the compiler can
    raise. Each member carries the template of its issue, and
    ``ETLErrorCode.X.issue(...)`` builds it.

``ETLCompilationError``
    Structured exception carrying ``code``, ``component``, ``field``, and a
    human-readable message. The compiler raises one per failure, built from
    an issue with :meth:`ETLCompilationError.from_issue`.

Where a message shows a list of names (``['a', 'b']``), the call site passes
that list already formatted, so the text stays as it was.
"""

from __future__ import annotations

from enum import unique
from typing import Final

from loom.core.errors.codes import CompileIssue, IssueCode, Template

_ALIAS: Final = "{alias}"
_ALIAS_REASON: Final = Template("{component}.{alias}: {reason}", field=_ALIAS)
"""Issue of a step attribute (a ``FromConfig`` key or a ``WithAgent``) with its reason."""


@unique
class ETLErrorCode(IssueCode):
    """Catalogue of every compiler validation failure and the issue it renders."""

    INVALID_PARAMS_TYPE = (
        "INVALID_PARAMS_TYPE",
        Template(
            "{component}.execute: first parameter must be 'params: {expected}'", field="params"
        ),
    )
    INVALID_PARAMS_NAME = (
        "INVALID_PARAMS_NAME",
        Template(
            "{component}.execute: first parameter must be named 'params', got '{got}'",
            field="{got}",
        ),
    )
    MISSING_SOURCE_PARAMS = (
        "MISSING_SOURCE_PARAMS",
        Template(
            "{component}.execute: source(s) {missing} declared in sources but missing as "
            "keyword-only parameter(s) after '*'"
        ),
    )
    EXTRA_SOURCE_PARAMS = (
        "EXTRA_SOURCE_PARAMS",
        Template(
            "{component}.execute: parameter(s) {extra} declared after '*' but not found in "
            "sources or FromConfig values"
        ),
    )
    MISSING_PARAMS_FIELDS = (
        "MISSING_PARAMS_FIELDS",
        Template("{component} requires params fields {missing} not present in {context}"),
    )
    UNKNOWN_SOURCE_TABLE = (
        "UNKNOWN_SOURCE_TABLE",
        Template(
            "{component}: source '{alias}' references unknown table '{table_ref}'", field=_ALIAS
        ),
    )
    UNKNOWN_TARGET_TABLE = (
        "UNKNOWN_TARGET_TABLE",
        Template("{component}: target references unknown table '{table_ref}'"),
    )
    TEMP_NOT_PRODUCED = (
        "TEMP_NOT_PRODUCED",
        Template(
            "{component}: source '{alias}' references FromTemp({temp_name!r}) but no prior "
            "IntoTemp({temp_name!r}) found in the plan",
            field=_ALIAS,
        ),
    )
    UPSERT_NO_KEYS = (
        "UPSERT_NO_KEYS",
        Template(
            "{component}: {method}() requires at least one key column. "
            'Pass keys=("col",) to identify rows uniquely.'
        ),
    )
    UPSERT_EXCLUDE_INCLUDE_CONFLICT = (
        "UPSERT_EXCLUDE_INCLUDE_CONFLICT",
        Template(
            "{component}: {method}() exclude= and include= are mutually exclusive. "
            "Use one or the other, not both."
        ),
    )
    UPSERT_KEY_IN_EXCLUDE = (
        "UPSERT_KEY_IN_EXCLUDE",
        Template(
            "{component}: {method}() exclude={overlap} overlaps with {method} keys — key "
            "columns are always excluded from UPDATE SET."
        ),
    )
    UPDATE_EMPTY_UPDATE_SET = (
        "UPDATE_EMPTY_UPDATE_SET",
        Template(
            "{component}: update() include={include} is fully covered by keys/partition_cols, "
            "which are always excluded from UPDATE SET — the MERGE would update no columns "
            "and the write would be a guaranteed no-op. List at least one non-key, "
            "non-partition column."
        ),
    )
    MISSING_GENERIC_PARAM = (
        "MISSING_GENERIC_PARAM",
        Template("{component}: missing generic parameter — use {kind}[YourParams] not bare {kind}"),
    )
    MISSING_TARGET = (
        "MISSING_TARGET",
        Template("{component}: 'target' is required but not declared"),
    )
    INVALID_TARGET_TYPE = (
        "INVALID_TARGET_TYPE",
        Template("{component}: 'target' must be IntoTable(...), IntoFile(...), or IntoTemp(...)"),
    )
    INVALID_PROCESS_ITEM = (
        "INVALID_PROCESS_ITEM",
        Template("{component}.processes: expected ETLProcess subclass or list thereof, got {item}"),
    )
    INVALID_STEP_ITEM = (
        "INVALID_STEP_ITEM",
        Template("{component}.steps: expected ETLStep subclass or list thereof, got {item}"),
    )
    DUPLICATE_TEMP_NAME = (
        "DUPLICATE_TEMP_NAME",
        Template(
            "{component}: IntoTemp({name!r}) already written by a prior step. Use append=True "
            "on all writers to enable fan-in, or choose a unique name for each intermediate.",
            field="{name}",
        ),
    )
    INVALID_TEMP_APPEND_MIX = (
        "INVALID_TEMP_APPEND_MIX",
        Template(
            "{component}: IntoTemp({name!r}) mixes append=True and append=False. All writers "
            "for the same intermediate must use the same mode.",
            field="{name}",
        ),
    )
    UNKNOWN_PARAM_FIELD = (
        "UNKNOWN_PARAM_FIELD",
        Template(
            "{component}: params expression references unknown field '{field_name}' on "
            "{params_type}",
            field="{field_name}",
        ),
    )
    UNKNOWN_TEMPLATE_FIELD = (
        "UNKNOWN_TEMPLATE_FIELD",
        Template(
            "{component}: file path template {path!r} references unknown field "
            "'{field_name}' on {params_type}",
            field="{field_name}",
        ),
    )
    MISSING_CONFIG_PARAMS = (
        "MISSING_CONFIG_PARAMS",
        Template(
            "{component}.execute: {marker} value(s) {missing} declared on the step but "
            "missing as keyword-only parameter(s) after '*'"
        ),
    )
    CONFIG_ALIAS_CONFLICT = (
        "CONFIG_ALIAS_CONFLICT",
        Template(
            "{component}: {marker} value(s) {aliases} share their name with a source alias "
            "or the injected 'client'; rename the attribute"
        ),
    )
    UNSUPPORTED_CONFIG_VALUE = (
        "UNSUPPORTED_CONFIG_VALUE",
        Template(
            "{component}: a StepSQL renders SQL and never calls execute(), so it cannot "
            "receive {marker} values"
        ),
    )
    # ``reason`` must never carry the value: callers pass the message of a
    # ``loom.etl.runtime.ConfigValueError``.
    UNRESOLVED_CONFIG_VALUE = "UNRESOLVED_CONFIG_VALUE", _ALIAS_REASON
    AGENT_NOT_FOUND = "AGENT_NOT_FOUND", _ALIAS_REASON
    AGENT_RUNNER_NOT_CONFIGURED = (
        "AGENT_RUNNER_NOT_CONFIGURED",
        Template(
            "{component}.{alias}: no agent runner is configured; declare an 'ai:' section "
            "in the runner config",
            field=_ALIAS,
        ),
    )
    AGENT_COMPILATION_FAILED = "AGENT_COMPILATION_FAILED", _ALIAS_REASON
    AGENT_OUTPUT_MISMATCH = "AGENT_OUTPUT_MISMATCH", _ALIAS_REASON
    AGENT_UNPRICED_BUDGET = "AGENT_UNPRICED_BUDGET", _ALIAS_REASON
    AGENT_BUDGET_UNENFORCEABLE = "AGENT_BUDGET_UNENFORCEABLE", _ALIAS_REASON
    AGENT_UNSUPPORTED_ENGINE = "AGENT_UNSUPPORTED_ENGINE", _ALIAS_REASON
    AGENT_COLUMN_COLLISION = "AGENT_COLUMN_COLLISION", _ALIAS_REASON

    # IntoHistory rules, checked again at compile time for a HistorifySpec
    # built without the IntoHistory builder.
    HISTORIFY_KEYS_EMPTY = (
        "HISTORIFY_KEYS_EMPTY",
        Template(
            "{component}: IntoHistory 'keys' must contain at least one column name.",
            field="keys",
        ),
    )
    HISTORIFY_KEYS_TRACK_OVERLAP = (
        "HISTORIFY_KEYS_TRACK_OVERLAP",
        Template(
            "{component}: IntoHistory 'keys' and 'track' must not overlap. "
            "Conflicting columns: {overlap}",
            field="track",
        ),
    )
    HISTORIFY_EFFECTIVE_DATE_IN_TRACK = (
        "HISTORIFY_EFFECTIVE_DATE_IN_TRACK",
        Template(
            "{component}: IntoHistory 'effective_date' column {effective_date!r} cannot "
            "appear in 'track'. The effective_date is a temporal axis, not a dimension "
            "being tracked.",
            field="effective_date",
        ),
    )
    HISTORIFY_BOUNDARY_IN_KEYS = (
        "HISTORIFY_BOUNDARY_IN_KEYS",
        Template(
            "{component}: IntoHistory boundary columns {overlap} cannot appear in 'keys'.",
            field="valid_from",
        ),
    )
    HISTORIFY_BOUNDARY_IN_TRACK = (
        "HISTORIFY_BOUNDARY_IN_TRACK",
        Template(
            "{component}: IntoHistory boundary columns {overlap} cannot appear in 'track'.",
            field="track",
        ),
    )
    HISTORIFY_LOG_EFFECTIVE_DATE_NOT_COLUMN = (
        "HISTORIFY_LOG_EFFECTIVE_DATE_NOT_COLUMN",
        Template(
            "{component}: IntoHistory LOG mode requires 'effective_date' to be a column name "
            "(str), not a ParamExpr. In LOG mode each row carries its own timestamp.",
            field="effective_date",
        ),
    )
    HISTORIFY_DELETE_MARKER_REQUIRES_LOG = (
        "HISTORIFY_DELETE_MARKER_REQUIRES_LOG",
        Template(
            "{component}: IntoHistory 'delete_marker' requires LOG mode. "
            "SNAPSHOT deletions are governed by 'delete_policy'.",
            field="delete_marker",
        ),
    )
    HISTORIFY_DELETE_MARKER_DECLARED_COLUMN = (
        "HISTORIFY_DELETE_MARKER_DECLARED_COLUMN",
        Template(
            "{component}: IntoHistory 'delete_marker' {marker!r} must not name a key, track, "
            "overwrite, effective_date, boundary or partition_scope column.",
            field="delete_marker",
        ),
    )


class ETLCompilationError(Exception):
    """Raised when an ETL class fails structural validation.

    Attributes:
        code:      Machine-readable :class:`ETLErrorCode`.
        component: Qualified name of the step or component that failed.
        field:     Optional name of the specific field or parameter involved.

    Example::

        raise ETLCompilationError.from_issue(ETLErrorCode.MISSING_TARGET.issue(component=MyStep))
    """

    def __init__(
        self,
        code: ETLErrorCode,
        component: str,
        message: str,
        field: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.component = component
        self.field = field

    @classmethod
    def from_issue(cls, issue: CompileIssue[ETLErrorCode]) -> ETLCompilationError:
        """Return the error that reports *issue*."""
        return cls(
            code=issue.code, component=issue.component, message=issue.message, field=issue.field
        )
