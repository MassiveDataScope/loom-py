"""Unit tests for StorageConfig and convert_storage_config."""

from __future__ import annotations

from pathlib import Path

import msgspec
import pytest

from loom.core.model.redaction import Dsn, RedactedMap, Secret
from loom.etl.storage._config import (
    AuditConfig,
    CatalogConnection,
    CustomColumnDef,
    FilePathConfig,
    FileRoute,
    MissingTablePolicy,
    StorageConfig,
    StorageDefaults,
    StorageEngine,
    TablePathConfig,
    TableRoute,
    TempConfig,
    convert_storage_config,
)
from loom.etl.storage._file_locator import FileLocation, MappingFileLocator
from loom.etl.storage._locator import MappingLocator, PrefixLocator


def _default_path(uri: str) -> StorageDefaults:
    return StorageDefaults(table_path=TablePathConfig(uri=Dsn(uri)))


class TestStorageConfig:
    def test_defaults(self) -> None:
        config = StorageConfig()
        assert config.engine == "polars"
        assert config.missing_table_policy == MissingTablePolicy.SCHEMA_MODE
        assert config.catalogs == {}
        assert config.defaults == StorageDefaults()
        assert config.temp == TempConfig()
        assert config.tables == ()
        assert config.files == ()

    def test_engine_enum_values(self) -> None:
        assert StorageEngine.POLARS.value == "polars"
        assert StorageEngine.SPARK.value == "spark"

    def test_validate_accepts_minimal_path_defaults(self) -> None:
        config = StorageConfig(defaults=_default_path("s3://my-lake"))
        config.validate()

    def test_validate_rejects_local_checkpoint_root(self, tmp_path: Path) -> None:
        local_path = tmp_path / "loom-checkpoints"
        config = StorageConfig(temp=TempConfig(root=Dsn(str(local_path))))
        with pytest.raises(ValueError, match="cloud URI"):
            config.validate()

    @pytest.mark.parametrize("root", ["s3://", "gs://", "abfss://", "file:///tmp/loom"])
    def test_validate_rejects_invalid_checkpoint_cloud_uri(self, root: str) -> None:
        config = StorageConfig(temp=TempConfig(root=Dsn(root)))
        with pytest.raises(ValueError, match="cloud URI"):
            config.validate()

    def test_validate_accepts_r2_checkpoint_root(self) -> None:
        config = StorageConfig(temp=TempConfig(root=Dsn("r2://my-bucket/checkpoints")))
        config.validate()

    def test_validate_rejects_duplicate_table_names(self) -> None:
        config = StorageConfig(
            tables=(
                TableRoute(name="raw.orders", path=TablePathConfig(uri=Dsn("s3://raw/orders"))),
                TableRoute(name="raw.orders", path=TablePathConfig(uri=Dsn("s3://raw/orders-v2"))),
            )
        )
        with pytest.raises(ValueError, match="duplicate name"):
            config.validate()

    def test_validate_rejects_duplicate_file_names(self) -> None:
        config = StorageConfig(
            files=(
                FileRoute(name="daily.csv", path=FilePathConfig(uri=Dsn("s3://a/daily.csv"))),
                FileRoute(name="daily.csv", path=FilePathConfig(uri=Dsn("s3://b/daily.csv"))),
            )
        )
        with pytest.raises(ValueError, match="duplicate name"):
            config.validate()

    def test_validate_rejects_missing_route_destination(self) -> None:
        config = StorageConfig(tables=(TableRoute(name="raw.orders"),))
        with pytest.raises(ValueError, match="exactly one destination"):
            config.validate()

    def test_validate_rejects_route_with_both_ref_and_path(self) -> None:
        config = StorageConfig(
            tables=(
                TableRoute(
                    name="raw.orders",
                    ref="main.raw.orders",
                    path=TablePathConfig(uri=Dsn("s3://raw/orders")),
                ),
            )
        )
        with pytest.raises(ValueError, match="exactly one destination"):
            config.validate()

    def test_validate_rejects_invalid_ref_shape(self) -> None:
        config = StorageConfig(tables=(TableRoute(name="raw.orders", ref="orders"),))
        with pytest.raises(ValueError, match="schema.table"):
            config.validate()

    def test_validate_rejects_unknown_catalog_key(self) -> None:
        config = StorageConfig(
            tables=(TableRoute(name="raw.orders", ref="raw.orders", catalog="finance"),)
        )
        with pytest.raises(ValueError, match="not defined in storage.catalogs"):
            config.validate()

    def test_validate_accepts_two_part_ref_with_catalog_key(self) -> None:
        config = StorageConfig(
            catalogs={
                "default": CatalogConnection(
                    workspace=Dsn("https://dbc.example"),
                    token=Secret("token-123"),
                )
            },
            tables=(TableRoute(name="raw.orders", ref="raw.orders", catalog="default"),),
        )
        config.validate()

    def test_validate_accepts_three_part_ref_without_catalog_key(self) -> None:
        config = StorageConfig(
            engine="spark",
            tables=(TableRoute(name="raw.orders", ref="main.raw.orders"),),
        )
        config.validate()

    def test_validate_rejects_polars_three_part_ref_without_catalog_credentials(self) -> None:
        config = StorageConfig(tables=(TableRoute(name="raw.orders", ref="main.raw.orders"),))
        with pytest.raises(ValueError, match="Polars UC routes require credentials"):
            config.validate()

    def test_validate_rejects_polars_uc_catalog_with_empty_credentials(self) -> None:
        config = StorageConfig(
            catalogs={"main": CatalogConnection()},
            tables=(TableRoute(name="raw.orders", ref="main.raw.orders"),),
        )
        with pytest.raises(ValueError, match="non-empty workspace and token"):
            config.validate()

    def test_validate_accepts_polars_three_part_ref_with_catalog_alias(self) -> None:
        config = StorageConfig(
            catalogs={
                "unity": CatalogConnection(
                    workspace=Dsn("https://dbc.example"),
                    token=Secret("token-123"),
                )
            },
            tables=(TableRoute(name="raw.orders", ref="main.raw.orders", catalog="unity"),),
        )
        config.validate()

    @pytest.mark.parametrize(
        "config,error",
        [
            (
                StorageConfig(defaults=_default_path("s3://lake"), tables=()),
                None,
            ),
            (
                StorageConfig(
                    defaults=StorageDefaults(
                        table_path=TablePathConfig(uri=Dsn("s3://lake"), writer={"bad": 1})
                    )
                ),
                "writer",
            ),
            (
                StorageConfig(
                    defaults=StorageDefaults(
                        table_path=TablePathConfig(uri=Dsn("s3://lake"), commit={"bad": 1})
                    )
                ),
                "commit",
            ),
        ],
    )
    def test_validate_writer_and_commit_options(
        self,
        config: StorageConfig,
        error: str | None,
    ) -> None:
        if error is None:
            config.validate()
            return
        with pytest.raises(TypeError, match=error):
            config.validate()

    def test_validate_rejects_non_positive_target_file_size(self) -> None:
        config = StorageConfig(
            defaults=StorageDefaults(
                table_path=TablePathConfig(uri=Dsn("s3://lake"), target_file_size=0)
            )
        )
        with pytest.raises(ValueError, match="target_file_size"):
            config.validate()

    def test_has_catalog_routes(self) -> None:
        config = StorageConfig(tables=(TableRoute(name="raw.orders", ref="main.raw.orders"),))
        assert config.has_catalog_routes() is True
        assert config.has_path_routes() is False

    def test_has_path_routes(self) -> None:
        config = StorageConfig(defaults=_default_path("s3://my-lake"))
        assert config.has_catalog_routes() is False
        assert config.has_path_routes() is True

    @pytest.mark.parametrize(
        "config,expected_type",
        [
            (StorageConfig(defaults=_default_path("s3://lake")), PrefixLocator),
            (
                StorageConfig(
                    defaults=_default_path("s3://default"),
                    tables=(
                        TableRoute(
                            name="raw.orders",
                            path=TablePathConfig(uri=Dsn("s3://finance/raw/orders")),
                        ),
                    ),
                ),
                MappingLocator,
            ),
        ],
    )
    def test_to_path_locator(self, config: StorageConfig, expected_type: type[object]) -> None:
        assert isinstance(config.to_path_locator(), expected_type)

    def test_to_path_locator_without_path_routes_raises(self) -> None:
        config = StorageConfig()
        with pytest.raises(ValueError, match="no path routes configured"):
            config.to_path_locator()


class TestConvertStorageConfig:
    def test_convert_valid_shape(self) -> None:
        raw = {
            "engine": "polars",
            "temp": {
                "root": "s3://my-checkpoints",
                "storage_options": {"endpoint_url": "http://localhost:19190"},
            },
            "defaults": {
                "table_path": {
                    "uri": "s3://my-lake",
                    "storage_options": {"AWS_REGION": "eu-west-1"},
                    "target_file_size": 134217728,
                }
            },
            "tables": [
                {"name": "raw.orders", "path": {"uri": "s3://raw/orders"}},
                {"name": "sys.customers", "ref": "main.crm.customers"},
            ],
        }
        config = convert_storage_config(raw)
        assert isinstance(config, StorageConfig)
        assert config.engine == StorageEngine.POLARS.value
        assert config.temp.root == "s3://my-checkpoints"
        assert config.defaults.table_path is not None
        assert config.defaults.table_path.target_file_size == 134217728
        assert len(config.tables) == 2

    def test_convert_invalid_engine_raises(self) -> None:
        with pytest.raises(msgspec.ValidationError):
            convert_storage_config({"engine": "unknown"})

    def test_convert_invalid_field_type_raises(self) -> None:
        with pytest.raises(msgspec.ValidationError):
            convert_storage_config({"temp": {"storage_options": ["bad"]}})


class TestToFileLocator:
    def test_returns_none_when_no_files(self) -> None:
        config = StorageConfig()
        assert config.to_file_locator() is None

    def test_returns_mapping_locator_with_files(self) -> None:
        config = StorageConfig(
            files=(
                FileRoute(
                    name="events_raw",
                    path=FilePathConfig(uri=Dsn("s3://raw/events/")),
                ),
            )
        )
        locator = config.to_file_locator()
        assert isinstance(locator, MappingFileLocator)

    def test_locator_resolves_registered_alias(self) -> None:
        config = StorageConfig(
            files=(
                FileRoute(
                    name="events_raw",
                    path=FilePathConfig(
                        uri=Dsn("s3://raw/events/"),
                        storage_options=RedactedMap({"AWS_REGION": "eu-west-1"}),
                    ),
                ),
            )
        )
        locator = config.to_file_locator()
        assert locator is not None
        location = locator.locate("events_raw")
        assert location.uri_template == "s3://raw/events/"
        assert location.storage_options == {"AWS_REGION": "eu-west-1"}

    def test_locator_raises_on_unknown_alias(self) -> None:
        config = StorageConfig(
            files=(FileRoute(name="events_raw", path=FilePathConfig(uri=Dsn("s3://raw/events/"))),)
        )
        locator = config.to_file_locator()
        assert locator is not None
        with pytest.raises(KeyError, match="events_daily"):
            locator.locate("events_daily")


class TestMappingFileLocator:
    def test_locate_returns_correct_location(self) -> None:
        locator = MappingFileLocator(
            mapping={"events": FileLocation(uri_template="s3://raw/events/")}
        )
        loc = locator.locate("events")
        assert loc.uri_template == "s3://raw/events/"

    def test_locate_missing_alias_raises_key_error(self) -> None:
        locator = MappingFileLocator(
            mapping={"events": FileLocation(uri_template="s3://raw/events/")}
        )
        with pytest.raises(KeyError, match="reports"):
            locator.locate("reports")

    def test_error_message_lists_available_aliases(self) -> None:
        locator = MappingFileLocator(
            mapping={
                "events": FileLocation(uri_template="s3://raw/events/"),
                "exports": FileLocation(uri_template="s3://out/exports/"),
            }
        )
        with pytest.raises(KeyError, match="events"):
            locator.locate("missing")

    def test_storage_options_empty_by_default(self) -> None:
        loc = FileLocation(uri_template="s3://bucket/path/")
        assert loc.storage_options == {}

    def test_storage_options_preserved(self) -> None:
        loc = FileLocation(
            uri_template="s3://bucket/path/",
            storage_options={"AWS_REGION": "us-east-1"},
        )
        assert loc.storage_options == {"AWS_REGION": "us-east-1"}

    def test_target_file_size_preserved_on_location(self) -> None:
        loc = TablePathConfig(
            uri=Dsn("s3://bucket/path/"), target_file_size=268435456
        ).to_location()
        assert loc.target_file_size == 268435456


class TestAuditConfig:
    def test_audit_config_defaults(self) -> None:
        cfg = AuditConfig()
        assert cfg.enabled is False
        assert cfg.prefix == "_loom_"
        assert cfg.custom == ()

    def test_audit_config_enabled(self) -> None:
        cfg = AuditConfig(enabled=True)
        assert cfg.enabled is True
        assert cfg.prefix == "_loom_"

    def test_audit_config_custom_prefix(self) -> None:
        cfg = AuditConfig(enabled=True, prefix="_etl_")
        assert cfg.prefix == "_etl_"

    def test_custom_column_def_with_literal_value(self) -> None:
        col_def = CustomColumnDef(name="env", value="prod")
        assert col_def.name == "env"
        assert col_def.value == "prod"
        assert col_def.from_param is None

    def test_custom_column_def_with_from_param(self) -> None:
        col_def = CustomColumnDef(name="run_date", from_param="run_date")
        assert col_def.name == "run_date"
        assert col_def.value is None
        assert col_def.from_param == "run_date"

    def test_audit_config_decode_from_dict(self) -> None:
        raw = {
            "enabled": True,
            "prefix": "_loom_",
            "custom": [
                {"name": "env", "value": "prod"},
                {"name": "run_date", "from_param": "run_date"},
            ],
        }
        import msgspec

        cfg = msgspec.convert(raw, AuditConfig)
        assert cfg.enabled is True
        assert len(cfg.custom) == 2
        assert cfg.custom[0].name == "env"
        assert cfg.custom[0].value == "prod"
        assert cfg.custom[1].from_param == "run_date"

    def test_storage_config_has_audit_field(self) -> None:
        config = StorageConfig()
        assert isinstance(config.audit, AuditConfig)
        assert config.audit.enabled is False

    def test_storage_config_with_audit_enabled(self) -> None:
        config = StorageConfig(audit=AuditConfig(enabled=True))
        assert config.audit.enabled is True

    def test_convert_storage_config_with_audit(self) -> None:
        raw = {
            "audit": {
                "enabled": True,
                "prefix": "_loom_",
                "custom": [{"name": "env", "value": "prod"}],
            }
        }
        config = convert_storage_config(raw)
        assert config.audit.enabled is True
        assert config.audit.custom[0].name == "env"
