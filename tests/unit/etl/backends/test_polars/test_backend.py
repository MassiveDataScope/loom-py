"""Integration tests for PolarsSourceReader and PolarsTargetWriter."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable, write_deltalake

from loom.etl import ETLParams, col
from loom.etl.backends.polars import PolarsSourceReader, PolarsTargetWriter
from loom.etl.backends.polars._schema import PolarsPhysicalSchema, SchemaNotFoundError
from loom.etl.declarative._format import Format
from loom.etl.declarative.expr._params import params as p
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import FileSourceSpec, TableSourceSpec
from loom.etl.declarative.source._specs import ClickHouseSourceSpec
from loom.etl.declarative.target import IntoTable, SchemaMode
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._table import AppendSpec, ReplacePartitionsSpec, ReplaceSpec
from loom.etl.lineage._records import WriteContext
from loom.etl.schema._schema import ColumnSchema, LoomDtype, SchemaError
from loom.etl.storage._config import AuditConfig, CustomColumnDef, MissingTablePolicy
from loom.etl.storage._locator import MappingLocator, TableLocation

from .conftest import table_path


class _DateParams(ETLParams):
    run_date: date


class _MissingSchemaReader:
    def read_schema(self, _target: object) -> PolarsPhysicalSchema | None:
        return None


def _file_source_spec() -> FileSourceSpec:
    from loom.etl.declarative._format import Format

    return FileSourceSpec(alias="data", path="s3://bucket/data.csv", format=Format.CSV)


def _seed(root: Path, ref: str, data: pl.DataFrame) -> None:
    path = table_path(root, TableRef(ref))
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), data, mode="overwrite")


def _read_table(root: Path, ref: str) -> pl.DataFrame:
    path = table_path(root, TableRef(ref))
    return pl.scan_delta(str(path)).collect()


def _spec(ref: str, schema_mode: SchemaMode = SchemaMode.STRICT) -> ReplaceSpec:
    return ReplaceSpec(table_ref=TableRef(ref), schema_mode=schema_mode)


def _source_spec(ref: str) -> TableSourceSpec:
    return TableSourceSpec(alias="data", table_ref=TableRef(ref))


def test_reader_returns_lazy_frame(tmp_path: Path) -> None:
    _seed(tmp_path, "raw.orders", pl.DataFrame({"id": [1, 2]}))
    reader = PolarsSourceReader(tmp_path)
    result = reader.read(_source_spec("raw.orders"), None)
    assert isinstance(result, pl.LazyFrame)


def test_reader_reads_correct_data(tmp_path: Path) -> None:
    data = pl.DataFrame({"id": [1, 2, 3], "v": [10, 20, 30]})
    _seed(tmp_path, "raw.events", data)
    result = PolarsSourceReader(tmp_path).read(_source_spec("raw.events"), None).collect()
    assert result["id"].to_list() == [1, 2, 3]
    assert result["v"].to_list() == [10, 20, 30]


def test_reader_dispatches_clickhouse_source_through_base_reader(tmp_path: Path) -> None:
    class _FakeClickHouseReader:
        def __init__(self) -> None:
            self.calls: list[tuple[ClickHouseSourceSpec, object]] = []

        def read(self, spec: ClickHouseSourceSpec, params_instance: object) -> pl.LazyFrame:
            self.calls.append((spec, params_instance))
            return pl.DataFrame({"id": [1], "amount": [9.5]}).lazy()

    reader_impl = _FakeClickHouseReader()
    spec = ClickHouseSourceSpec(
        alias="clickhouse",
        table_ref=TableRef("analytics.cdc_events"),
        predicates=(),
        columns=("id", "amount"),
        schema=(),
        distinct=False,
        allow_full_scan=True,
    )

    reader = PolarsSourceReader(tmp_path, clickhouse_reader=reader_impl)
    result = reader.read(spec, None).collect()

    assert reader_impl.calls
    assert reader_impl.calls[0][0] == spec
    assert result.columns == ["id", "amount"]


def test_writer_strict_passes_with_matching_frame(tmp_path: Path) -> None:
    _seed(tmp_path, "staging.out", pl.DataFrame({"id": [0], "v": [0.0]}))
    writer = PolarsTargetWriter(tmp_path)
    frame = pl.DataFrame({"id": [1], "v": [1.0]}).lazy()
    writer.write(frame, _spec("staging.out", schema_mode=SchemaMode.STRICT), None)
    result = _read_table(tmp_path, "staging.out")
    assert result["id"].to_list() == [1]


def test_writer_strict_drops_extra_column(tmp_path: Path) -> None:
    _seed(tmp_path, "staging.out", pl.DataFrame({"id": [0]}))
    writer = PolarsTargetWriter(tmp_path)
    frame = pl.DataFrame({"id": [1], "extra": ["x"]}).lazy()
    writer.write(frame, _spec("staging.out", schema_mode=SchemaMode.STRICT), None)
    result = _read_table(tmp_path, "staging.out")
    assert "extra" not in result.columns
    assert result["id"].to_list() == [1]


def test_writer_raises_schema_not_found_when_table_missing(tmp_path: Path) -> None:
    writer = PolarsTargetWriter(tmp_path)
    frame = pl.DataFrame({"id": [1]}).lazy()
    spec = _spec("staging.new", schema_mode=SchemaMode.STRICT)
    with pytest.raises(SchemaNotFoundError):
        writer.write(frame, spec, None)


def test_writer_overwrite_creates_table_when_missing(tmp_path: Path) -> None:
    """OVERWRITE on a non-existent table creates it from the frame schema."""
    writer = PolarsTargetWriter(tmp_path)
    ref = TableRef("staging.new")
    frame = pl.DataFrame({"id": [1, 2], "v": [10, 20]}).lazy()
    writer.write(frame, _spec(ref.ref, schema_mode=SchemaMode.OVERWRITE), None)
    assert DeltaTable.is_deltatable(str(table_path(tmp_path, ref)))
    written = _read_table(tmp_path, ref.ref)
    assert written.columns == ["id", "v"]


def test_writer_evolve_fills_missing_columns(tmp_path: Path) -> None:
    """EVOLVE: columns missing from frame are written as nulls."""
    _seed(tmp_path, "staging.out", pl.DataFrame({"id": [0], "label": ["x"]}))
    writer = PolarsTargetWriter(tmp_path)
    frame = pl.DataFrame({"id": [1]}).lazy()  # label missing
    writer.write(frame, _spec("staging.out", schema_mode=SchemaMode.EVOLVE), None)
    result = _read_table(tmp_path, "staging.out")
    assert "label" in result.columns
    written_rows = result.filter(pl.col("id") == 1)
    assert written_rows["label"][0] is None


def test_writer_reads_latest_schema_between_consecutive_writes(tmp_path: Path) -> None:
    writer = PolarsTargetWriter(tmp_path)
    ref = TableRef("staging.evolving")

    writer.write(
        pl.DataFrame({"id": [1]}).lazy(),
        ReplaceSpec(table_ref=ref, schema_mode=SchemaMode.OVERWRITE),
        None,
    )
    write_deltalake(
        str(table_path(tmp_path, ref)),
        pl.DataFrame({"id": [2], "amount": [20.0]}),
        mode="overwrite",
        schema_mode="overwrite",
    )
    writer.write(
        pl.DataFrame({"id": [3], "amount": [30.0]}).lazy(),
        AppendSpec(table_ref=ref, schema_mode=SchemaMode.STRICT),
        None,
    )

    result = _read_table(tmp_path, ref.ref).sort("id")
    assert result.columns == ["id", "amount"]
    assert result["id"].to_list() == [2, 3]
    assert result["amount"].to_list() == [20.0, 30.0]


@pytest.mark.parametrize("mode", [SchemaMode.STRICT, SchemaMode.EVOLVE])
def test_writer_refuses_a_struct_into_a_string_column_and_leaves_the_table(
    tmp_path: Path, mode: SchemaMode
) -> None:
    _seed(tmp_path, "staging.answers", pl.DataFrame({"id": [0], "offer": ['{"amount":1}']}))
    frame = pl.DataFrame({"id": [1], "offer": [{"amount": 1.0}]}).lazy()
    spec = AppendSpec(table_ref=TableRef("staging.answers"), schema_mode=mode)

    with pytest.raises(SchemaError, match=r"'offer'"):
        PolarsTargetWriter(tmp_path).write(frame, spec, None)

    assert _read_table(tmp_path, "staging.answers").to_dicts() == [
        {"id": 0, "offer": '{"amount":1}'}
    ]


def test_reader_reads_csv_file(tmp_path: Path) -> None:
    """FILE sources are now supported — reader dispatches to _read_file."""
    from loom.etl.declarative._format import Format

    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,amount\n1,9.99\n2,19.99\n")
    spec = FileSourceSpec(alias="data", path=str(csv_path), format=Format.CSV)
    reader = PolarsSourceReader(tmp_path)
    lf = reader.read(spec, None)
    df = lf.collect()
    assert df.shape == (2, 2)
    assert list(df.columns) == ["id", "amount"]


def test_reader_applies_source_schema_on_csv(tmp_path: Path) -> None:
    """with_schema() casts declared columns at read time."""
    from loom.etl.declarative._format import Format

    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,amount\n1,9.99\n2,19.99\n")
    schema = (
        ColumnSchema("id", LoomDtype.INT64),
        ColumnSchema("amount", LoomDtype.FLOAT64),
    )
    spec = FileSourceSpec(alias="data", path=str(csv_path), format=Format.CSV, schema=schema)
    reader = PolarsSourceReader(tmp_path)
    df = reader.read(spec, None).collect()
    assert df["id"].dtype == pl.Int64
    assert df["amount"].dtype == pl.Float64


def test_writer_writes_csv_file(tmp_path: Path) -> None:
    """FILE targets (CSV) are now supported."""
    out_path = tmp_path / "out.csv"
    frame = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]}).lazy()
    writer = PolarsTargetWriter(tmp_path)
    writer.write(frame, _file_target_spec_local(str(out_path)), None)
    result = pl.read_csv(str(out_path))
    assert result.shape == (2, 2)


def _file_target_spec_local(path: str) -> FileSpec:
    return FileSpec(path=path, format=Format.CSV)


def test_writer_append_adds_rows(tmp_path: Path) -> None:
    _seed(tmp_path, "staging.ledger", pl.DataFrame({"id": [0], "v": [0.0]}))
    writer = PolarsTargetWriter(tmp_path)
    frame = pl.DataFrame({"id": [1, 2], "v": [1.0, 2.0]}).lazy()
    writer.write(frame, AppendSpec(table_ref=TableRef("staging.ledger")), None)
    result = _read_table(tmp_path, "staging.ledger")
    assert result.height == 3


def test_writer_append_first_write_requires_create_policy(tmp_path: Path) -> None:
    writer = PolarsTargetWriter(tmp_path)
    frame = pl.DataFrame({"id": [1], "v": [10.0]}).lazy()

    table_ref = TableRef("staging.append_first")
    with pytest.raises(SchemaNotFoundError, match="missing_table_policy='create'"):
        writer.append(frame, table_ref, None)


def test_writer_error_policy_blocks_creation_even_with_overwrite_schema(tmp_path: Path) -> None:
    writer = PolarsTargetWriter(tmp_path, missing_table_policy=MissingTablePolicy.ERROR)
    frame = pl.DataFrame({"id": [1], "v": [10.0]}).lazy()
    spec = ReplaceSpec(table_ref=TableRef("staging.must_exist"), schema_mode=SchemaMode.OVERWRITE)

    with pytest.raises(SchemaNotFoundError):
        writer.write(frame, spec, None)


def test_writer_append_creates_table_on_first_write(tmp_path: Path) -> None:
    writer = PolarsTargetWriter(tmp_path, missing_table_policy=MissingTablePolicy.CREATE)
    frame = pl.DataFrame({"id": [1], "v": [10.0]}).lazy()

    writer.append(frame, TableRef("staging.append_first"), None)

    result = _read_table(tmp_path, "staging.append_first")
    assert result.shape == (1, 2)


def test_writer_warns_on_polars_uc_first_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom.etl.backends.polars import _writer as polars_writer_module

    writes: list[tuple[str, str]] = []

    def _fake_write_deltalake(table_or_uri: str, _data: object, **kwargs: object) -> None:
        mode = kwargs.get("mode")
        writes.append((table_or_uri, str(mode)))

    monkeypatch.setattr(polars_writer_module, "write_deltalake", _fake_write_deltalake)
    monkeypatch.setattr(polars_writer_module, "read_delta_physical_schema", lambda *_args: None)

    locator = MappingLocator(
        mapping={
            "raw.uc_orders": TableLocation(
                uri="uc://main.raw.orders",
                storage_options={
                    "databricks_workspace_url": "https://dbc.example",
                    "databricks_access_token": "token-123",
                },
            )
        }
    )
    writer = PolarsTargetWriter(
        locator,
        missing_table_policy=MissingTablePolicy.CREATE,
    )
    warnings: list[tuple[str, dict[str, object]]] = []

    def _capture(event: str, **fields: object) -> None:
        warnings.append((event, fields))

    monkeypatch.setattr(polars_writer_module._log, "warning", _capture)
    writer.append(pl.DataFrame({"id": [1]}).lazy(), TableRef("raw.uc_orders"), None)

    assert writes == [("uc://main.raw.orders", "overwrite")]
    assert any("catalog registration is not guaranteed" in event for event, _ in warnings)


def test_write_kwargs_include_target_file_size() -> None:
    loc = TableLocation(
        uri="s3://lake/raw/orders",
        storage_options={"AWS_REGION": "eu-west-1"},
        writer={"compression": "ZSTD"},
        target_file_size=268435456,
        delta_config={"delta.appendOnly": "true"},
        commit={"custom_metadata": {"pipeline": "motos"}},
    )

    kwargs = PolarsTargetWriter._write_kwargs(loc)

    assert kwargs["target_file_size"] == 268435456
    assert kwargs["storage_options"] == {"AWS_REGION": "eu-west-1"}
    assert kwargs["writer_properties"] is not None
    assert kwargs["commit_properties"] is not None


def test_writer_replace_partitions_overwrites_matching_partition(tmp_path: Path) -> None:
    initial = pl.DataFrame({"year": [2023, 2024], "v": [10, 20]})
    _seed(tmp_path, "staging.facts", initial)
    writer = PolarsTargetWriter(tmp_path)
    new_data = pl.DataFrame({"year": [2024], "v": [99]})
    spec = ReplacePartitionsSpec(
        table_ref=TableRef("staging.facts"),
        partition_cols=("year",),
    )
    writer.write(new_data.lazy(), spec, None)
    result = _read_table(tmp_path, "staging.facts")
    assert result.filter(pl.col("year") == 2024)["v"].to_list() == [99]
    assert result.filter(pl.col("year") == 2023)["v"].to_list() == [10]


def test_writer_replace_physical_partitions_refuses_an_unpartitioned_table(
    tmp_path: Path,
) -> None:
    initial = pl.DataFrame({"year": [2023, 2024], "v": [10, 20]})
    _seed(tmp_path, "staging.flatfacts", initial)  # sin particion fisica
    writer = PolarsTargetWriter(tmp_path)
    spec = ReplacePartitionsSpec(
        table_ref=TableRef("staging.flatfacts"),
        partition_cols=("year",),
        require_physical=True,
    )
    frame = pl.DataFrame({"year": [2024], "v": [99]}).lazy()

    with pytest.raises(ValueError, match="partitioned by nothing but the write asked"):
        writer.write(frame, spec, None)


def test_writer_replace_physical_partitions_refuses_an_unpartitioned_table_when_streaming(
    tmp_path: Path,
) -> None:
    initial = pl.DataFrame({"year": [2023, 2024], "v": [10, 20]})
    _seed(tmp_path, "staging.streamflat", initial)
    writer = PolarsTargetWriter(tmp_path)
    spec = ReplacePartitionsSpec(
        table_ref=TableRef("staging.streamflat"),
        partition_cols=("year",),
        require_physical=True,
        streaming=True,
    )
    frame = pl.DataFrame({"year": [2024], "v": [99]}).lazy()

    with pytest.raises(ValueError, match="partitioned by nothing but the write asked"):
        writer.write(frame, spec, None)


def test_writer_replace_physical_partitions_writes_on_a_partitioned_table(
    tmp_path: Path,
) -> None:
    path = table_path(tmp_path, TableRef("staging.partfacts"))
    path.mkdir(parents=True, exist_ok=True)
    initial = pl.DataFrame({"year": [2023, 2024], "v": [10, 20]})
    write_deltalake(str(path), initial, mode="overwrite", partition_by=["year"])
    writer = PolarsTargetWriter(tmp_path)
    spec = ReplacePartitionsSpec(
        table_ref=TableRef("staging.partfacts"),
        partition_cols=("year",),
        require_physical=True,
    )
    writer.write(pl.DataFrame({"year": [2024], "v": [99]}).lazy(), spec, None)
    result = _read_table(tmp_path, "staging.partfacts")
    assert result.filter(pl.col("year") == 2024)["v"].to_list() == [99]
    assert result.filter(pl.col("year") == 2023)["v"].to_list() == [10]


def test_writer_replace_partitions_with_string_values(tmp_path: Path) -> None:
    initial = pl.DataFrame({"region": ["us", "eu"], "v": [1, 2]})
    _seed(tmp_path, "staging.regions", initial)
    writer = PolarsTargetWriter(tmp_path)
    new_data = pl.DataFrame({"region": ["us"], "v": [99]})
    spec = ReplacePartitionsSpec(
        table_ref=TableRef("staging.regions"),
        partition_cols=("region",),
    )
    writer.write(new_data.lazy(), spec, None)
    result = _read_table(tmp_path, "staging.regions")
    assert result.filter(pl.col("region") == "us")["v"].to_list() == [99]
    assert result.filter(pl.col("region") == "eu")["v"].to_list() == [2]


def test_writer_replace_partitions_with_bool_values(tmp_path: Path) -> None:
    initial = pl.DataFrame({"active": [True, False], "v": [1, 2]})
    _seed(tmp_path, "staging.flags", initial)
    writer = PolarsTargetWriter(tmp_path)
    new_data = pl.DataFrame({"active": [True], "v": [99]})
    spec = ReplacePartitionsSpec(
        table_ref=TableRef("staging.flags"),
        partition_cols=("active",),
    )
    writer.write(new_data.lazy(), spec, None)
    result = _read_table(tmp_path, "staging.flags")
    assert result.filter(pl.col("active"))["v"].to_list() == [99]
    assert result.filter(~pl.col("active"))["v"].to_list() == [2]


def test_writer_replace_partitions_first_run_creates_partitioned_table(tmp_path: Path) -> None:
    writer = PolarsTargetWriter(tmp_path)
    new_data = pl.DataFrame({"year": [2024], "v": [99]})
    spec = ReplacePartitionsSpec(
        table_ref=TableRef("staging.partitioned"),
        partition_cols=("year",),
        schema_mode=SchemaMode.OVERWRITE,
    )
    writer.write(new_data.lazy(), spec, None)

    dt = DeltaTable(str(table_path(tmp_path, TableRef("staging.partitioned"))))
    assert dt.metadata().partition_columns == ["year"]


def test_writer_replace_partitions_first_run_empty_frame_missing_partition_cols_skips(
    tmp_path: Path,
) -> None:
    """Empty frame whose schema omits the partition columns must be a no-op.

    This is the case produced by decomposition fallbacks that emit an empty
    frame with only the root id column when the source field is absent. The
    create path used to surface a cryptic delta-rs error ("Partition column(s)
    not found in write schema"); it should silently skip instead, mirroring
    ``_replace_partitions`` on the update path.
    """
    writer = PolarsTargetWriter(tmp_path)
    # No "year" column at all — empty schema, empty rows. Mimics _empty_frame().
    empty = pl.DataFrame({"transaction_id": []}, schema={"transaction_id": pl.Utf8})
    spec = ReplacePartitionsSpec(
        table_ref=TableRef("staging.empty_no_partition_cols"),
        partition_cols=("year",),
        schema_mode=SchemaMode.OVERWRITE,
    )

    writer.write(empty.lazy(), spec, None)

    target_dir = table_path(tmp_path, TableRef("staging.empty_no_partition_cols"))
    assert not target_dir.exists() or not any(target_dir.iterdir())


def test_writer_replace_partitions_first_run_missing_partition_cols_raises_loom_error(
    tmp_path: Path,
) -> None:
    """Non-empty frame missing partition columns must raise a clear loom error.

    Without this guard the delta-rs error ``Partition column(s) not found in
    write schema`` surfaces to callers and gives no hint that the frame is the
    cause.
    """
    writer = PolarsTargetWriter(tmp_path)
    # Has data, but partition column "year" is absent from the schema.
    frame = pl.DataFrame({"v": [1, 2]})
    spec = ReplacePartitionsSpec(
        table_ref=TableRef("staging.missing_partition"),
        partition_cols=("year",),
        schema_mode=SchemaMode.OVERWRITE,
    )

    lazy_frame = frame.lazy()
    with pytest.raises(ValueError, match="partition columns.*not found.*year"):
        writer.write(lazy_frame, spec, None)


def test_writer_replace_where_overwrites_matching_rows(tmp_path: Path) -> None:
    initial = pl.DataFrame({"year": [2023, 2024], "v": [10, 20]})
    _seed(tmp_path, "staging.yearly", initial)
    writer = PolarsTargetWriter(tmp_path)
    new_data = pl.DataFrame({"year": [2024], "v": [99]})
    pred = col("year") == p.run_date.year
    spec = IntoTable("staging.yearly").replace_where(pred)._to_spec()
    writer.write(new_data.lazy(), spec, _DateParams(run_date=date(2024, 1, 1)))
    result = _read_table(tmp_path, "staging.yearly")
    assert result.filter(pl.col("year") == 2024)["v"].to_list() == [99]
    assert result.filter(pl.col("year") == 2023)["v"].to_list() == [10]


def test_writer_replace_where_creates_the_table_when_it_is_absent(tmp_path: Path) -> None:
    writer = PolarsTargetWriter(tmp_path)
    new_data = pl.DataFrame({"year": [2024], "v": [99]})
    declared = IntoTable("staging.fresh").replace_where(col("year") == p.run_date.year)
    spec = replace(declared._to_spec(), schema_mode=SchemaMode.OVERWRITE)
    writer.write(new_data.lazy(), spec, _DateParams(run_date=date(2024, 1, 1)))
    assert _read_table(tmp_path, "staging.fresh")["v"].to_list() == [99]


def test_writer_persists_schema_after_write(tmp_path: Path) -> None:
    """Physical table schema reflects the written frame after a successful write."""
    _seed(tmp_path, "staging.out", pl.DataFrame({"id": [0]}))
    writer = PolarsTargetWriter(tmp_path)
    frame = pl.DataFrame({"id": [1]}).lazy()
    writer.write(frame, _spec("staging.out", schema_mode=SchemaMode.STRICT), None)
    result = _read_table(tmp_path, "staging.out")
    assert result.schema["id"] == pl.Int64


class TestFileLocatorAliasResolution:
    """Alias resolution for FromFile.alias() / IntoFile.alias() via FileLocator."""

    def test_reader_resolves_alias_to_physical_path(self, tmp_path: Path) -> None:
        from loom.etl.storage._file_locator import FileLocation, MappingFileLocator

        csv_path = tmp_path / "data.csv"
        csv_path.write_text("id,v\n1,10\n2,20\n")

        locator = MappingFileLocator(
            mapping={"events_raw": FileLocation(uri_template=str(csv_path))}
        )
        reader = PolarsSourceReader(tmp_path, file_locator=locator)
        spec = FileSourceSpec(
            alias="events_raw", path="events_raw", format=Format.CSV, is_alias=True
        )
        df = reader.read(spec, None).collect()
        assert df.shape == (2, 2)

    def test_reader_raises_when_alias_missing_locator(self, tmp_path: Path) -> None:
        reader = PolarsSourceReader(tmp_path)
        spec = FileSourceSpec(
            alias="events_raw", path="events_raw", format=Format.CSV, is_alias=True
        )
        with pytest.raises(ValueError, match="storage.files"):
            reader.read(spec, None)

    def test_writer_resolves_alias_to_physical_path(self, tmp_path: Path) -> None:
        from loom.etl.storage._file_locator import FileLocation, MappingFileLocator

        out_path = tmp_path / "out.csv"
        locator = MappingFileLocator(
            mapping={"exports_daily": FileLocation(uri_template=str(out_path))}
        )
        writer = PolarsTargetWriter(tmp_path, file_locator=locator)
        frame = pl.DataFrame({"id": [1, 2]}).lazy()
        spec = FileSpec(path="exports_daily", format=Format.CSV, is_alias=True)
        writer.write(frame, spec, None)
        assert out_path.exists()

    def test_writer_raises_when_alias_missing_locator(self, tmp_path: Path) -> None:
        writer = PolarsTargetWriter(tmp_path)
        frame = pl.DataFrame({"id": [1]}).lazy()
        spec = FileSpec(path="exports_daily", format=Format.CSV, is_alias=True)
        with pytest.raises(ValueError, match="storage.files"):
            writer.write(frame, spec, None)


def _audit_write_ctx(*, process_run_id: str | None = "proc-1") -> WriteContext:
    return WriteContext(
        run_id="run-abc",
        step="StepA",
        attempt=1,
        process_run_id=process_run_id,
    )


class TestApplyAuditColumns:
    """Unit tests for PolarsTargetWriter._apply_audit_columns hook."""

    def test_disabled_audit_returns_frame_unchanged(self, tmp_path: Path) -> None:
        writer = PolarsTargetWriter(str(tmp_path))
        frame = pl.DataFrame({"id": [1]}).lazy()
        audit = AuditConfig(enabled=False)
        result = writer._apply_audit_columns(frame, _audit_write_ctx(), None, audit)
        assert result.collect().columns == ["id"]

    def test_none_write_ctx_returns_frame_unchanged(self, tmp_path: Path) -> None:
        writer = PolarsTargetWriter(str(tmp_path))
        frame = pl.DataFrame({"id": [1]}).lazy()
        audit = AuditConfig(enabled=True)
        result = writer._apply_audit_columns(frame, None, None, audit)
        assert result.collect().columns == ["id"]

    def test_enabled_audit_injects_loom_columns(self, tmp_path: Path) -> None:
        writer = PolarsTargetWriter(str(tmp_path))
        frame = pl.DataFrame({"id": [1]}).lazy()
        audit = AuditConfig(enabled=True)
        result = writer._apply_audit_columns(frame, _audit_write_ctx(), None, audit).collect()
        assert "_loom_run_id" in result.columns
        assert "_loom_step" in result.columns
        assert "_loom_attempt" in result.columns
        assert result["_loom_run_id"][0] == "run-abc"
        assert result["_loom_step"][0] == "StepA"
        assert result["_loom_attempt"][0] == 1
        assert result["_loom_attempt"].dtype == pl.Int32

    def test_enabled_audit_injects_process_run_id_when_present(self, tmp_path: Path) -> None:
        writer = PolarsTargetWriter(str(tmp_path))
        frame = pl.DataFrame({"id": [1]}).lazy()
        audit = AuditConfig(enabled=True)
        result = writer._apply_audit_columns(
            frame, _audit_write_ctx(process_run_id="proc-xyz"), None, audit
        ).collect()
        assert "_loom_process_run_id" in result.columns
        assert result["_loom_process_run_id"][0] == "proc-xyz"

    def test_enabled_audit_skips_process_run_id_when_none(self, tmp_path: Path) -> None:
        writer = PolarsTargetWriter(str(tmp_path))
        frame = pl.DataFrame({"id": [1]}).lazy()
        audit = AuditConfig(enabled=True)
        result = writer._apply_audit_columns(
            frame, _audit_write_ctx(process_run_id=None), None, audit
        ).collect()
        assert "_loom_process_run_id" not in result.columns

    def test_custom_literal_column_is_added(self, tmp_path: Path) -> None:
        writer = PolarsTargetWriter(str(tmp_path))
        frame = pl.DataFrame({"id": [1]}).lazy()
        audit = AuditConfig(
            enabled=True,
            custom=(CustomColumnDef(name="env", value="prod"),),
        )
        result = writer._apply_audit_columns(frame, _audit_write_ctx(), None, audit).collect()
        assert "env" in result.columns
        assert result["env"][0] == "prod"

    def test_custom_from_param_column_reads_param_attribute(self, tmp_path: Path) -> None:
        from datetime import date

        class _Params:
            run_date = date(2024, 1, 15)

        writer = PolarsTargetWriter(str(tmp_path))
        frame = pl.DataFrame({"id": [1]}).lazy()
        audit = AuditConfig(
            enabled=True,
            custom=(CustomColumnDef(name="run_date", from_param="run_date"),),
        )
        result = writer._apply_audit_columns(frame, _audit_write_ctx(), _Params(), audit).collect()
        assert "run_date" in result.columns
        assert result["run_date"][0] == "2024-01-15"

    def test_custom_prefix_is_used(self, tmp_path: Path) -> None:
        writer = PolarsTargetWriter(str(tmp_path))
        frame = pl.DataFrame({"id": [1]}).lazy()
        audit = AuditConfig(enabled=True, prefix="_etl_")
        result = writer._apply_audit_columns(frame, _audit_write_ctx(), None, audit).collect()
        assert "_etl_run_id" in result.columns
        assert "_loom_run_id" not in result.columns
