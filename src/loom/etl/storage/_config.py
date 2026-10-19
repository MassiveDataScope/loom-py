"""Unified ETL storage configuration.

Defines one canonical ``storage:`` model for both Spark and Polars runtimes.
Logical table names remain stable in pipelines while this config resolves each
name to either a catalog reference or a physical Delta path.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final, Literal
from urllib.parse import urlparse

if TYPE_CHECKING:
    from loom.etl.storage._file_locator import MappingFileLocator

import msgspec
from deltalake import CommitProperties, WriterProperties

from loom.core.model import LoomFrozenStruct
from loom.etl._uri import CLOUD_SCHEMES
from loom.etl.storage._locator import MappingLocator, PrefixLocator, TableLocation, TableLocator

STORAGE_KEYED_COLLECTIONS: Final = ("storage.tables", "storage.files", "storage.profiles")
"""Dotted paths of the ``storage:`` collections merged by key across ``includes``."""


class StorageEngine(StrEnum):
    """Execution engine guardrail declared in the storage config."""

    POLARS = "polars"
    SPARK = "spark"


class MissingTablePolicy(StrEnum):
    """Policy used when a TABLE target does not exist at write time."""

    SCHEMA_MODE = "schema_mode"
    CREATE = "create"
    ERROR = "error"


class CatalogConnection(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Catalog connection settings.

    Args:
        provider: Catalog provider. Currently ``"unity"``.
        workspace: Databricks workspace URL for Unity Catalog.
        token: Databricks access token.
    """

    provider: Literal["unity"] = "unity"
    workspace: str = ""
    token: str = ""


class StorageProfile(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Named partial physical-path settings shared by routes through ``profile:``.

    A profile never carries ``uri``. Table routes and ``defaults.table_path``
    take every field; file routes take ``storage_options`` only.

    Args:
        storage_options: Object-store credentials/options.
        writer: Delta writer properties.
        target_file_size: Optional target output file size in bytes.
        delta_config: Delta table properties.
        commit: Delta commit metadata.
    """

    storage_options: dict[str, str] = {}
    writer: dict[str, Any] = {}
    target_file_size: int | None = None
    delta_config: dict[str, str | None] = {}
    commit: dict[str, Any] = {}


class TablePathConfig(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Physical Delta path settings for table storage.

    Args:
        uri: Root Delta path/URI.
        storage_options: Object-store credentials/options.
        writer: Delta writer properties.
        target_file_size: Optional target output file size in bytes.
        delta_config: Delta table properties.
        commit: Delta commit metadata.
    """

    uri: str = ""
    storage_options: dict[str, str] = {}
    writer: dict[str, Any] = {}
    target_file_size: int | None = None
    delta_config: dict[str, str | None] = {}
    commit: dict[str, Any] = {}

    def validate(self, *, context: str) -> None:
        """Validate path config and delta-rs option dictionaries."""
        if not self.uri.strip():
            raise ValueError(f"storage.{context}.uri must be a non-empty string")
        _validate_writer(self.writer, context=f"{context}.writer")
        _validate_target_file_size(self.target_file_size, context=f"{context}.target_file_size")
        _validate_commit(self.commit, context=f"{context}.commit")

    def to_location(self) -> TableLocation:
        """Convert to :class:`TableLocation`."""
        return TableLocation(
            uri=self.uri,
            storage_options=self.storage_options,
            writer=self.writer,
            target_file_size=self.target_file_size,
            delta_config=self.delta_config,
            commit=self.commit,
        )


class FilePathConfig(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Physical path settings for FILE sources/targets.

    Args:
        uri: Output/input file URI template.
        storage_options: Object-store credentials/options.
    """

    uri: str = ""
    storage_options: dict[str, str] = {}

    def validate(self, *, context: str) -> None:
        """Validate file path config."""
        if not self.uri.strip():
            raise ValueError(f"storage.{context}.uri must be a non-empty string")


_ROUTE_PROFILE_FIELDS: Final[dict[str, frozenset[str]]] = {
    "tables": frozenset(TablePathConfig.__struct_fields__) - {"uri"},
    "files": frozenset(FilePathConfig.__struct_fields__) - {"uri"},
}
"""Route collections normalised by key, each with the profile fields it takes."""


class StorageDefaults(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Default resolution settings used when no per-name override is declared."""

    table_path: TablePathConfig | None = None


class TempConfig(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Checkpoint / temporary storage settings."""

    root: str = ""
    storage_options: dict[str, str] = {}

    def validate(self) -> None:
        if self.root and not _is_cloud_uri(self.root):
            raise ValueError(
                "storage.temp.root must be a cloud URI (s3://, gs://, abfss://, ...). "
                "Local checkpoint roots are not supported."
            )


class TableRoute(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Route one logical table name to catalog ref or physical path.

    Args:
        name: Logical table name used by the pipeline (e.g. ``"sys.customers"``).
        ref: Catalog reference. Supported forms:
            * ``"catalog.schema.table"``
            * ``"schema.table"`` (uses ``catalog`` or ``default``)
        catalog: Catalog connection key used with 2-part refs.
        path: Physical Delta path route.
    """

    name: str
    ref: str = ""
    catalog: str = ""
    path: TablePathConfig | None = None

    def validate(self, *, catalogs: dict[str, CatalogConnection], context: str) -> None:
        """Validate route syntax and exclusivity constraints."""
        if not self.name.strip():
            raise ValueError(f"storage.{context}.name must be a non-empty string")

        has_ref = bool(self.ref.strip())
        has_path = self.path is not None
        if has_ref == has_path:
            raise ValueError(
                f"storage.{context} must define exactly one destination: 'ref' or 'path'"
            )

        if has_ref:
            _validate_ref(self.ref, context=f"storage.{context}.ref")
            if self.catalog and self.catalog not in catalogs:
                raise ValueError(
                    f"storage.{context}.catalog={self.catalog!r} is not defined in storage.catalogs"
                )

        if self.path is not None:
            self.path.validate(context=f"{context}.path")


class FileRoute(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Route one logical file name to a physical file URI.

    Args:
        name: Logical file name used by the pipeline.
        path: Physical path configuration.
    """

    name: str
    path: FilePathConfig

    def validate(self, *, context: str) -> None:
        """Validate file route fields."""
        if not self.name.strip():
            raise ValueError(f"storage.{context}.name must be a non-empty string")
        self.path.validate(context=f"{context}.path")


class ClickHouseConfig(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Connection settings for ClickHouse targets.

    Args:
        url: ClickHouse DSN, e.g. ``clickhouse://user:pass@host:8123/default``.
             When empty, ClickHouse write targets are not available.
    """

    url: str = ""


class MongoConfig(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Connection settings for MongoDB sources.

    Args:
        uri:      pymongo connection string, e.g. ``mongodb://user:pass@host:27017``.
                  When empty, MongoDB read sources are not available.
        database: Database name to read from.
    """

    uri: str = ""
    database: str = ""


class DynamoDbConfig(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Connection settings for DynamoDB sources.

    Args:
        enabled:      Activate DynamoDB read sources.  When ``False`` (default),
                      no boto3 client is built at provider wiring time.
        region:       AWS region name, e.g. ``eu-west-1``.  Empty uses the
                      default boto3 resolution chain.
        endpoint_url: Custom endpoint for DynamoDB Local / LocalStack.
                      Empty uses the real AWS endpoint.
        profile:      AWS named profile.  Empty uses the default credential
                      resolution chain.
    """

    enabled: bool = False
    region: str = ""
    endpoint_url: str = ""
    profile: str = ""


class CustomColumnDef(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Definition of one custom audit column injected at write time.

    Exactly one of ``value`` or ``from_param`` must be set.

    Args:
        name:       Column name to add to the written frame.
        value:      Literal string value (static, same for every row/run).
        from_param: Attribute name on the params instance; resolved at
                    runtime via ``str(getattr(params, from_param))``.
    """

    name: str
    value: str | None = None
    from_param: str | None = None


class AuditConfig(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Configuration for automatic audit columns injected at write time.

    When ``enabled=True`` the writer stamps each written frame with
    ``{prefix}run_id``, ``{prefix}step``, ``{prefix}attempt``, and
    ``{prefix}process_run_id`` (only when not ``None``).  Additional
    custom columns are appended afterwards.

    Args:
        enabled: Activate audit-column injection.  Defaults to ``False``.
        prefix:  Column name prefix for the standard loom columns.
                 Defaults to ``"_loom_"``.
        custom:  Extra columns to append, resolved from a literal value or
                 a params attribute at runtime.
    """

    enabled: bool = False
    prefix: str = "_loom_"
    custom: tuple[CustomColumnDef, ...] = ()


class MaintenanceVacuumConfig(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Vacuum settings for the config-driven maintenance block.

    Args:
        retention_hours: Files older than this are eligible for deletion.
            ``None`` uses the table's ``delta.deletedFileRetentionDuration``
            (default 168 h / 7 days).
        dry_run: When ``True`` (default), lists files without deleting.
            Set ``dry_run=False`` explicitly for production runs.
        full: When ``True``, also deletes every file under the table
            directory that the Delta log does not reference and that is
            older than the retention, whoever wrote it; only names starting
            with an underscore are kept.  ``False`` (default) deletes only
            the files the log records as removed.
    """

    retention_hours: int | None = None
    dry_run: bool = True
    full: bool = False


class MaintenanceConfig(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Config-driven Delta table maintenance settings.

    Declaring this section in the storage YAML allows :meth:`MaintenanceRunner.run_from_config`
    to execute maintenance without any Python class::

        maintenance:
          schemas:
            - raw
            - staging
          vacuum:
            retention_hours: 168
            dry_run: false
          compact: true

    Args:
        schemas: Schema prefixes to discover tables from (e.g. ``["raw", "staging"]``).
            Matches all ``StorageConfig.tables`` routes whose name starts with
            ``{schema}.``.  An empty list means all configured tables.
        vacuum: Vacuum settings to apply to all discovered tables.
            ``None`` skips vacuum.
        compact: When ``True``, run compaction on all discovered tables.
            Mutually exclusive with ``z_order_by``.
        z_order_by: Columns to Z-Order by.  Empty tuple skips Z-Order.
            Mutually exclusive with ``compact``.
    """

    schemas: tuple[str, ...] = ()
    vacuum: MaintenanceVacuumConfig | None = None
    compact: bool = False
    z_order_by: tuple[str, ...] = ()


class StorageConfig(LoomFrozenStruct, frozen=True, forbid_unknown_fields=True):
    """Canonical storage configuration used by ETL runner/factory.

    Args:
        engine: Engine guardrail.
        missing_table_policy: First-run policy for missing destination tables.
        catalogs: Catalog connection map.
        defaults: Default path settings.
        temp: Checkpoint / temporary storage settings.
        tables: Per-logical-table routes.
        files: Per-logical-file routes.
        profiles: Named partial path settings referenced by ``profile:`` in YAML.
            Applied by :func:`normalise_storage_section`; direct struct
            construction does not apply them.
        audit: Audit-column injection settings.
    """

    engine: Literal["polars", "spark"] = "polars"
    missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE
    catalogs: dict[str, CatalogConnection] = {}
    defaults: StorageDefaults = StorageDefaults()
    temp: TempConfig = TempConfig()
    tables: tuple[TableRoute, ...] = ()
    files: tuple[FileRoute, ...] = ()
    profiles: dict[str, StorageProfile] = {}
    clickhouse: ClickHouseConfig = ClickHouseConfig()
    mongo: MongoConfig = MongoConfig()
    dynamodb: DynamoDbConfig = DynamoDbConfig()
    audit: AuditConfig = AuditConfig()
    maintenance: MaintenanceConfig = MaintenanceConfig()

    def validate(self) -> None:
        """Validate structural constraints and option dictionaries."""
        _ensure_unique_names(
            (route.name for route in self.tables),
            context="storage.tables",
        )
        _ensure_unique_names(
            (route.name for route in self.files),
            context="storage.files",
        )

        if self.defaults.table_path is not None:
            self.defaults.table_path.validate(context="defaults.table_path")
        self.temp.validate()

        for idx, table_route in enumerate(self.tables):
            table_route.validate(catalogs=self.catalogs, context=f"tables[{idx}]")
        for idx, file_route in enumerate(self.files):
            file_route.validate(context=f"files[{idx}]")
        self._validate_polars_uc_credentials()

    @property
    def checkpoint_root(self) -> str:
        """Checkpoint root URI."""
        return self.temp.root

    @property
    def checkpoint_storage_options(self) -> dict[str, str]:
        """Checkpoint storage options."""
        return self.temp.storage_options

    def has_catalog_routes(self) -> bool:
        """Return ``True`` when at least one table route uses ``ref``."""
        return any(bool(route.ref.strip()) for route in self.tables)

    def to_file_locator(self) -> MappingFileLocator | None:
        """Build a :class:`~loom.etl.storage._file_locator.MappingFileLocator`
        from the declared ``storage.files`` routes, or ``None`` when no file
        routes are configured.

        Returns:
            Locator mapping each ``files[].name`` to its physical
            :class:`~loom.etl.storage._file_locator.FileLocation`, or
            ``None`` when ``storage.files`` is empty.
        """
        from loom.etl.storage._file_locator import FileLocation, MappingFileLocator

        if not self.files:
            return None
        mapping = {
            route.name: FileLocation(
                uri_template=route.path.uri,
                storage_options=route.path.storage_options,
            )
            for route in self.files
        }
        return MappingFileLocator(mapping)

    def has_path_routes(self) -> bool:
        """Return ``True`` when defaults or per-table routes use path mode."""
        if self.defaults.table_path is not None:
            return True
        return any(route.path is not None for route in self.tables)

    def to_path_locator(self) -> TableLocator:
        """Build a :class:`TableLocator` from path defaults and per-table path routes.

        Raises:
            ValueError: If no path defaults/routes are configured.
        """
        mapping = {
            route.name: route.path.to_location() for route in self.tables if route.path is not None
        }
        default_location = (
            self.defaults.table_path.to_location() if self.defaults.table_path is not None else None
        )
        if not mapping and default_location is None:
            raise ValueError(
                "storage.to_path_locator: no path routes configured. "
                "Define storage.defaults.table_path or storage.tables[].path."
            )
        if not mapping and default_location is not None:
            return PrefixLocator(
                root=default_location.uri,
                storage_options=default_location.storage_options or None,
                writer=default_location.writer or None,
                delta_config=default_location.delta_config or None,
                commit=default_location.commit or None,
            )
        return MappingLocator(mapping=mapping, default=default_location)

    def _validate_polars_uc_credentials(self) -> None:
        if self.engine != StorageEngine.POLARS.value:
            return
        for idx, table_route in enumerate(self.tables):
            if not table_route.ref.strip():
                continue
            catalog_key = _resolve_polars_catalog_key(table_route, self.catalogs)
            if not catalog_key:
                raise ValueError(
                    "Polars UC routes require credentials in storage.catalogs. "
                    f"Missing mapping for storage.tables[{idx}] (ref={table_route.ref!r}). "
                    "Set tables[].catalog or define matching storage.catalogs entry."
                )
            connection = self.catalogs[catalog_key]
            if not connection.workspace.strip() or not connection.token.strip():
                raise ValueError(
                    f"storage.catalogs[{catalog_key!r}] must define non-empty workspace and token "
                    "for Polars UC routes."
                )


def convert_storage_config(raw: dict[str, Any]) -> StorageConfig:
    """Convert a resolved plain dict into :class:`StorageConfig`."""
    return msgspec.convert(raw, StorageConfig)


def normalise_storage_section(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Normalise a raw ``storage:`` section into the shape :class:`StorageConfig` binds to.

    ``tables`` and ``files`` given as a mapping keyed by logical name become a
    list of routes carrying ``name``; the list form is returned unchanged.
    A ``path`` block carrying ``profile: <name>`` is completed with the fields
    of ``profiles[<name>]`` it does not set itself and loses the ``profile``
    key. Values that are not mappings are left untouched so that conversion
    reports them.

    Args:
        raw: Resolved ``storage:`` section.

    Returns:
        A new dict ready for :func:`convert_storage_config`.

    Raises:
        ValueError: When a mapping entry carries a ``name`` different from its
            key, when a ``profile`` is not declared in ``profiles``, or when a
            profile carries ``uri``. Other fields unknown to
            :class:`StorageProfile` are reported by :func:`convert_storage_config`.
    """
    normalised = dict(raw)
    for collection in _ROUTE_PROFILE_FIELDS:
        if collection in normalised:
            normalised[collection] = _normalise_routes(normalised[collection], collection)
    return _apply_profiles(normalised)


def _apply_profiles(normalised: dict[str, Any]) -> dict[str, Any]:
    profiles = normalised.get("profiles")
    profiles = profiles if isinstance(profiles, Mapping) else {}
    result = dict(normalised)
    for collection, fields in _ROUTE_PROFILE_FIELDS.items():
        if isinstance(result.get(collection), list):
            result[collection] = [
                _route_with_profile(
                    route, f"storage.{collection}[{_route_key(route, idx)}]", profiles, fields
                )
                for idx, route in enumerate(result[collection])
            ]
    defaults = result.get("defaults")
    if isinstance(defaults, Mapping) and isinstance(defaults.get("table_path"), Mapping):
        table_path = _path_with_profile(
            defaults["table_path"],
            "storage.defaults.table_path",
            profiles,
            _ROUTE_PROFILE_FIELDS["tables"],
        )
        result["defaults"] = {**defaults, "table_path": table_path}
    return result


def _route_key(route: Any, idx: int) -> str:
    if isinstance(route, Mapping) and "name" in route:
        return repr(route["name"])
    return str(idx)


def _route_with_profile(
    route: Any,
    context: str,
    profiles: Mapping[str, Any],
    fields: frozenset[str],
) -> Any:
    if not isinstance(route, Mapping) or not isinstance(route.get("path"), Mapping):
        return route
    return {**route, "path": _path_with_profile(route["path"], f"{context}.path", profiles, fields)}


def _path_with_profile(
    path: Mapping[str, Any],
    context: str,
    profiles: Mapping[str, Any],
    fields: frozenset[str],
) -> dict[str, Any]:
    if "profile" not in path:
        return dict(path)
    own = {key: value for key, value in path.items() if key != "profile"}
    profile = _resolve_profile(path["profile"], context, profiles)
    inherited = {key: value for key, value in profile.items() if key in fields}
    return {**inherited, **own}


def _resolve_profile(name: Any, context: str, profiles: Mapping[str, Any]) -> dict[str, Any]:
    if name not in profiles:
        raise ValueError(f"{context}.profile={name!r} is not defined in storage.profiles")
    declared = profiles[name]
    if not isinstance(declared, Mapping):
        return {}
    if "uri" in declared:
        raise ValueError(f"storage.profiles[{name!r}] must not define uri")
    return dict(declared)


def _normalise_routes(routes: Any, collection: str) -> Any:
    if not isinstance(routes, Mapping):
        return routes
    return [_route_from_entry(key, value, collection) for key, value in routes.items()]


def _route_from_entry(key: Any, value: Any, collection: str) -> Any:
    if not isinstance(value, Mapping):
        return value
    inner_name = value.get("name", key)
    if inner_name != key:
        raise ValueError(
            f"storage.{collection}[{key!r}] declares name {inner_name!r}; "
            "the mapping key is the route name"
        )
    return {"name": key, **value}


def _validate_ref(ref: str, *, context: str) -> None:
    parts = tuple(part for part in ref.split(".") if part)
    if len(parts) not in (2, 3):
        raise ValueError(f"{context} must be 'schema.table' or 'catalog.schema.table', got {ref!r}")


def _ensure_unique_names(names: Any, *, context: str) -> None:
    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise ValueError(f"{context} contains duplicate name {name!r}")
        seen.add(name)


def _resolve_polars_catalog_key(
    route: TableRoute,
    catalogs: Mapping[str, CatalogConnection],
) -> str:
    explicit_key = route.catalog.strip()
    if explicit_key:
        return explicit_key

    parts = tuple(part for part in route.ref.split(".") if part)
    if len(parts) == 2:
        return "default" if "default" in catalogs else ""
    if len(parts) == 3:
        catalog_from_ref = parts[0]
        return catalog_from_ref if catalog_from_ref in catalogs else ""
    return ""


def _is_cloud_uri(path: str) -> bool:
    parsed = urlparse(path.strip())
    if parsed.scheme.lower() not in CLOUD_SCHEMES:
        return False
    # Cloud roots must include authority/bucket/container to avoid accepting
    # incomplete URIs like "s3://".
    return bool(parsed.netloc)


# ---------------------------------------------------------------------------
# Internal validators — construct delta-rs objects to fail fast at startup
# ---------------------------------------------------------------------------


def _validate_writer(writer: dict[str, Any], *, context: str) -> None:
    if not writer:
        return
    try:
        WriterProperties(**writer)
    except TypeError as exc:
        raise TypeError(f"Invalid key in storage.{context}: {exc}") from exc


def _validate_target_file_size(target_file_size: int | None, *, context: str) -> None:
    if target_file_size is None:
        return
    if target_file_size <= 0:
        raise ValueError(f"storage.{context} must be a positive integer when set")


def _validate_commit(commit: dict[str, Any], *, context: str) -> None:
    if not commit:
        return
    try:
        CommitProperties(**commit)
    except TypeError as exc:
        raise TypeError(f"Invalid key in storage.{context}: {exc}") from exc
