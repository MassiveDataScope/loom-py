"""ETL compilation error types.

``ETLErrorCode``
    Machine-readable enum of every validation failure the compiler can raise.

``ETLCompilationError``
    Structured exception carrying ``code``, ``component``, ``field``, and a
    human-readable ``message``.  Each error code has a dedicated class-method
    factory so call-sites stay intention-revealing and free of string formatting.
"""

from __future__ import annotations

from enum import StrEnum


class ETLErrorCode(StrEnum):
    """Enumeration of all compiler validation failure codes."""

    INVALID_PARAMS_TYPE = "INVALID_PARAMS_TYPE"
    INVALID_PARAMS_NAME = "INVALID_PARAMS_NAME"
    MISSING_SOURCE_PARAMS = "MISSING_SOURCE_PARAMS"
    EXTRA_SOURCE_PARAMS = "EXTRA_SOURCE_PARAMS"
    MISSING_PARAMS_FIELDS = "MISSING_PARAMS_FIELDS"
    UNKNOWN_SOURCE_TABLE = "UNKNOWN_SOURCE_TABLE"
    UNKNOWN_TARGET_TABLE = "UNKNOWN_TARGET_TABLE"
    TEMP_NOT_PRODUCED = "TEMP_NOT_PRODUCED"
    UPSERT_NO_KEYS = "UPSERT_NO_KEYS"
    UPSERT_EXCLUDE_INCLUDE_CONFLICT = "UPSERT_EXCLUDE_INCLUDE_CONFLICT"
    UPSERT_KEY_IN_EXCLUDE = "UPSERT_KEY_IN_EXCLUDE"
    UPDATE_EMPTY_UPDATE_SET = "UPDATE_EMPTY_UPDATE_SET"
    MISSING_GENERIC_PARAM = "MISSING_GENERIC_PARAM"
    MISSING_TARGET = "MISSING_TARGET"
    INVALID_TARGET_TYPE = "INVALID_TARGET_TYPE"
    INVALID_SOURCES_TYPE = "INVALID_SOURCES_TYPE"
    INVALID_PROCESS_ITEM = "INVALID_PROCESS_ITEM"
    INVALID_STEP_ITEM = "INVALID_STEP_ITEM"
    DUPLICATE_TEMP_NAME = "DUPLICATE_TEMP_NAME"
    INVALID_TEMP_APPEND_MIX = "INVALID_TEMP_APPEND_MIX"
    UNKNOWN_PARAM_FIELD = "UNKNOWN_PARAM_FIELD"
    UNKNOWN_TEMPLATE_FIELD = "UNKNOWN_TEMPLATE_FIELD"
    MISSING_CONFIG_PARAMS = "MISSING_CONFIG_PARAMS"
    CONFIG_ALIAS_CONFLICT = "CONFIG_ALIAS_CONFLICT"
    UNSUPPORTED_CONFIG_VALUE = "UNSUPPORTED_CONFIG_VALUE"
    UNRESOLVED_CONFIG_VALUE = "UNRESOLVED_CONFIG_VALUE"
    AGENT_NOT_FOUND = "AGENT_NOT_FOUND"
    AGENT_COMPILATION_FAILED = "AGENT_COMPILATION_FAILED"
    AGENT_OUTPUT_MISMATCH = "AGENT_OUTPUT_MISMATCH"
    AGENT_UNPRICED_BUDGET = "AGENT_UNPRICED_BUDGET"
    AGENT_BUDGET_UNENFORCEABLE = "AGENT_BUDGET_UNENFORCEABLE"


class ETLCompilationError(Exception):
    """Raised when an ETL class fails structural validation.

    Attributes:
        code:      Machine-readable :class:`ETLErrorCode`.
        component: Qualified name of the step or component that failed.
        field:     Optional name of the specific field or parameter involved.

    Use the factory class-methods to construct instances — they own all message
    formatting so call-sites stay readable.

    Example::

        raise ETLCompilationError.upsert_no_keys(MyStep)
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

    # ------------------------------------------------------------------
    # Factory methods — one per ETLErrorCode
    # ------------------------------------------------------------------

    @classmethod
    def invalid_params_type(cls, step: type, expected: type) -> ETLCompilationError:
        """First execute() parameter has wrong type annotation."""
        return cls(
            code=ETLErrorCode.INVALID_PARAMS_TYPE,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}.execute: first parameter must be "
                f"'params: {expected.__name__}'"
            ),
            field="params",
        )

    @classmethod
    def invalid_params_name(cls, step: type, got: str) -> ETLCompilationError:
        """First execute() parameter is not named 'params'."""
        return cls(
            code=ETLErrorCode.INVALID_PARAMS_NAME,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}.execute: first parameter must be named 'params', got '{got}'"
            ),
            field=got,
        )

    @classmethod
    def missing_source_params(cls, step: type, missing: frozenset[str]) -> ETLCompilationError:
        """Sources declared in sources but absent as kw-only execute() parameters."""
        return cls(
            code=ETLErrorCode.MISSING_SOURCE_PARAMS,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}.execute: source(s) {sorted(missing)} "
                "declared in sources but missing as keyword-only parameter(s) after '*'"
            ),
        )

    @classmethod
    def extra_source_params(cls, step: type, extra: frozenset[str]) -> ETLCompilationError:
        """Kw-only execute() parameters not declared in sources nor as FromConfig."""
        return cls(
            code=ETLErrorCode.EXTRA_SOURCE_PARAMS,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}.execute: parameter(s) {sorted(extra)} "
                "declared after '*' but not found in sources or FromConfig values"
            ),
        )

    @classmethod
    def missing_params_fields(
        cls, component: type, missing: frozenset[str], context: type
    ) -> ETLCompilationError:
        """Component requires params fields that the context params type lacks."""
        return cls(
            code=ETLErrorCode.MISSING_PARAMS_FIELDS,
            component=component.__qualname__,
            message=(
                f"{component.__qualname__} requires params fields "
                f"{sorted(missing)} not present in {context.__name__}"
            ),
        )

    @classmethod
    def unknown_source_table(cls, step: type, alias: str, table_ref: str) -> ETLCompilationError:
        """Source binding references a table not in the catalog."""
        return cls(
            code=ETLErrorCode.UNKNOWN_SOURCE_TABLE,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: source '{alias}' references unknown table '{table_ref}'"
            ),
            field=alias,
        )

    @classmethod
    def unknown_target_table(cls, step: type, table_ref: str) -> ETLCompilationError:
        """Target references a table not in the catalog."""
        return cls(
            code=ETLErrorCode.UNKNOWN_TARGET_TABLE,
            component=step.__qualname__,
            message=(f"{step.__qualname__}: target references unknown table '{table_ref}'"),
        )

    @classmethod
    def temp_not_produced(cls, step: type, alias: str, temp_name: str) -> ETLCompilationError:
        """FromTemp references a temp name with no prior IntoTemp."""
        return cls(
            code=ETLErrorCode.TEMP_NOT_PRODUCED,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: source '{alias}' "
                f"references FromTemp({temp_name!r}) but no prior "
                f"IntoTemp({temp_name!r}) found in the plan"
            ),
            field=alias,
        )

    @classmethod
    def upsert_no_keys(cls, step: type, method: str = "upsert") -> ETLCompilationError:
        """upsert()/update() declared with no key columns."""
        return cls(
            code=ETLErrorCode.UPSERT_NO_KEYS,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: {method}() requires at least one key column. "
                'Pass keys=("col",) to identify rows uniquely.'
            ),
        )

    @classmethod
    def upsert_exclude_include_conflict(
        cls, step: type, method: str = "upsert"
    ) -> ETLCompilationError:
        """upsert()/update() has both exclude= and include= set."""
        return cls(
            code=ETLErrorCode.UPSERT_EXCLUDE_INCLUDE_CONFLICT,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: {method}() exclude= and include= are mutually exclusive. "
                "Use one or the other, not both."
            ),
        )

    @classmethod
    def upsert_key_in_exclude(
        cls, step: type, overlap: frozenset[str], method: str = "upsert"
    ) -> ETLCompilationError:
        """upsert()/update() exclude= contains columns that are also merge keys."""
        return cls(
            code=ETLErrorCode.UPSERT_KEY_IN_EXCLUDE,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: {method}() exclude={sorted(overlap)} overlaps with "
                f"{method} keys — key columns are always excluded from UPDATE SET."
            ),
        )

    @classmethod
    def update_empty_update_set(cls, step: type, include: tuple[str, ...]) -> ETLCompilationError:
        """update() include= is fully absorbed by keys and partition columns."""
        return cls(
            code=ETLErrorCode.UPDATE_EMPTY_UPDATE_SET,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: update() include={sorted(include)} is fully covered "
                "by keys/partition_cols, which are always excluded from UPDATE SET — "
                "the MERGE would update no columns and the write would be a guaranteed "
                "no-op. List at least one non-key, non-partition column."
            ),
        )

    @classmethod
    def missing_generic_param(cls, cls_type: type, kind: str) -> ETLCompilationError:
        """ETLStep/ETLProcess/ETLPipeline used without a generic parameter."""
        return cls(
            code=ETLErrorCode.MISSING_GENERIC_PARAM,
            component=cls_type.__qualname__,
            message=(
                f"{cls_type.__qualname__}: missing generic parameter — "
                f"use {kind}[YourParams] not bare {kind}"
            ),
        )

    @classmethod
    def missing_target(cls, step: type) -> ETLCompilationError:
        """'target' is required but not declared on the step."""
        return cls(
            code=ETLErrorCode.MISSING_TARGET,
            component=step.__qualname__,
            message=f"{step.__qualname__}: 'target' is required but not declared",
        )

    @classmethod
    def invalid_target_type(cls, step: type) -> ETLCompilationError:
        """'target' is not an IntoTable/IntoFile/IntoTemp instance."""
        return cls(
            code=ETLErrorCode.INVALID_TARGET_TYPE,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: 'target' must be "
                "IntoTable(...), IntoFile(...), or IntoTemp(...)"
            ),
        )

    @classmethod
    def invalid_sources_type(cls, step: type) -> ETLCompilationError:
        """'sources' is not a Sources(...) or SourceSet instance."""
        return cls(
            code=ETLErrorCode.INVALID_SOURCES_TYPE,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: 'sources' must be Sources(...) or a SourceSet instance"
            ),
        )

    @classmethod
    def invalid_process_item(cls, pipeline: type, item: object) -> ETLCompilationError:
        """An item in pipeline.processes is not an ETLProcess subclass."""
        return cls(
            code=ETLErrorCode.INVALID_PROCESS_ITEM,
            component=pipeline.__qualname__,
            message=(
                f"{pipeline.__qualname__}.processes: "
                f"expected ETLProcess subclass or list thereof, got {item!r}"
            ),
        )

    @classmethod
    def invalid_step_item(cls, process: type, item: object) -> ETLCompilationError:
        """An item in process.steps is not an ETLStep subclass."""
        return cls(
            code=ETLErrorCode.INVALID_STEP_ITEM,
            component=process.__qualname__,
            message=(
                f"{process.__qualname__}.steps: "
                f"expected ETLStep subclass or list thereof, got {item!r}"
            ),
        )

    @classmethod
    def duplicate_temp_name(cls, step: type, name: str) -> ETLCompilationError:
        """Two steps write to the same IntoTemp name without append=True."""
        return cls(
            code=ETLErrorCode.DUPLICATE_TEMP_NAME,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: IntoTemp({name!r}) already written by a prior step. "
                "Use append=True on all writers to enable fan-in, "
                "or choose a unique name for each intermediate."
            ),
            field=name,
        )

    @classmethod
    def invalid_temp_append_mix(cls, step: type, name: str) -> ETLCompilationError:
        """A step mixes append=True and append=False on the same IntoTemp name."""
        return cls(
            code=ETLErrorCode.INVALID_TEMP_APPEND_MIX,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: IntoTemp({name!r}) mixes append=True and append=False. "
                "All writers for the same intermediate must use the same mode."
            ),
            field=name,
        )

    @classmethod
    def unknown_template_field(
        cls, step: type, field_name: str, path: str, params_type: type
    ) -> ETLCompilationError:
        """A file path template references a field not declared on the params type."""
        return cls(
            code=ETLErrorCode.UNKNOWN_TEMPLATE_FIELD,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: file path template {path!r} references unknown "
                f"field '{field_name}' on {params_type.__name__}"
            ),
            field=field_name,
        )

    @classmethod
    def unknown_param_field(
        cls, step: type, field_name: str, params_type: type
    ) -> ETLCompilationError:
        """A ParamExpr references a field not declared on the params type."""
        return cls(
            code=ETLErrorCode.UNKNOWN_PARAM_FIELD,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: params expression references unknown field "
                f"'{field_name}' on {params_type.__name__}"
            ),
            field=field_name,
        )

    @classmethod
    def missing_config_params(
        cls, step: type, missing: frozenset[str], marker: str = "FromConfig"
    ) -> ETLCompilationError:
        """FromConfig or WithAgent attributes absent as kw-only execute() parameters."""
        return cls(
            code=ETLErrorCode.MISSING_CONFIG_PARAMS,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}.execute: {marker} value(s) {sorted(missing)} "
                "declared on the step but missing as keyword-only parameter(s) after '*'"
            ),
        )

    @classmethod
    def config_alias_conflict(
        cls, step: type, aliases: frozenset[str], marker: str = "FromConfig"
    ) -> ETLCompilationError:
        """A FromConfig or WithAgent attribute shares its name with a source or the client."""
        return cls(
            code=ETLErrorCode.CONFIG_ALIAS_CONFLICT,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: {marker} value(s) {sorted(aliases)} share their "
                "name with a source alias or the injected 'client'; rename the attribute"
            ),
        )

    @classmethod
    def unsupported_config_value(
        cls, step: type, marker: str = "FromConfig"
    ) -> ETLCompilationError:
        """A StepSQL declares FromConfig or WithAgent values it has no execute() to receive."""
        return cls(
            code=ETLErrorCode.UNSUPPORTED_CONFIG_VALUE,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: a StepSQL renders SQL and never calls execute(), "
                f"so it cannot receive {marker} values"
            ),
        )

    @classmethod
    def unresolved_config_value(cls, step: type, alias: str, reason: str) -> ETLCompilationError:
        """A FromConfig key cannot be supplied from the runner's config.

        *reason* must never carry the value: callers pass the message of a
        :class:`~loom.etl.runtime.ConfigValueError`.
        """
        return cls(
            code=ETLErrorCode.UNRESOLVED_CONFIG_VALUE,
            component=step.__qualname__,
            message=f"{step.__qualname__}.{alias}: {reason}",
            field=alias,
        )

    @classmethod
    def agent_not_found(cls, step: type, alias: str, reason: str) -> ETLCompilationError:
        """A WithAgent names an agent no configured artifact declares."""
        return cls._agent(ETLErrorCode.AGENT_NOT_FOUND, step, alias, reason)

    @classmethod
    def agent_compilation_failed(cls, step: type, alias: str, reason: str) -> ETLCompilationError:
        """The artifact a WithAgent names does not compile; *reason* carries its issues."""
        return cls._agent(ETLErrorCode.AGENT_COMPILATION_FAILED, step, alias, reason)

    @classmethod
    def agent_output_mismatch(cls, step: type, alias: str, reason: str) -> ETLCompilationError:
        """A WithAgent output differs from the ``type_ref`` output of its artifact."""
        return cls._agent(ETLErrorCode.AGENT_OUTPUT_MISMATCH, step, alias, reason)

    @classmethod
    def agent_unpriced_budget(cls, step: type, alias: str, reason: str) -> ETLCompilationError:
        """A budget is declared for an agent whose model has no known price."""
        return cls._agent(ETLErrorCode.AGENT_UNPRICED_BUDGET, step, alias, reason)

    @classmethod
    def agent_budget_unenforceable(cls, step: type, alias: str, reason: str) -> ETLCompilationError:
        """A step budget no worst case of one run can be reserved against."""
        return cls._agent(ETLErrorCode.AGENT_BUDGET_UNENFORCEABLE, step, alias, reason)

    @classmethod
    def _agent(cls, code: ETLErrorCode, step: type, alias: str, reason: str) -> ETLCompilationError:
        return cls(
            code=code,
            component=step.__qualname__,
            message=f"{step.__qualname__}.{alias}: {reason}",
            field=alias,
        )
