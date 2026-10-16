"""Unit tests for Polars checkpoint append schema alignment."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import fsspec.core as fsspec_core
import polars as pl
import pytest

from loom.etl.checkpoint._backends._polars import _PolarsCheckpointBackend


def test_append_aligns_to_existing_schema(tmp_path: Path) -> None:
    backend = _PolarsCheckpointBackend(storage_options={})
    base = str(tmp_path / "checkpoints")

    # Create base directory only (cloud storage doesn't need pre-created subdirs)
    os.makedirs(base, exist_ok=True)

    first = pl.DataFrame({"id": [1], "amount": [10.5]}).lazy()
    second = pl.DataFrame({"id": ["2"], "extra": ["x"]}).lazy()

    backend.write("orders", base, first, append=True)
    backend.write("orders", base, second, append=True)

    scanned = backend.probe("orders", base)
    assert scanned is not None
    out = scanned.collect().sort("id")

    assert out.columns == ["id", "amount"]
    assert out.schema["id"] == pl.Int64
    assert out.schema["amount"] == pl.Float64
    assert out.to_dict(as_series=False) == {"id": [1, 2], "amount": [10.5, None]}


def test_write_passes_encryption_to_sink_and_rename(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both halves of the atomic write must carry the declared SSE options.

    ``sink_ipc`` writes the temp object through object_store and the rename is a
    server-side copy through fsspec; if either misses the CMK the surviving
    object ends up under the bucket default key.
    """
    seen_fsspec: dict[str, object] = {}
    mock_fs = MagicMock()

    def _url_to_fs(_path: str, **kwargs: object) -> tuple[object, str]:
        seen_fsspec.update(kwargs)
        return mock_fs, "/bucket/tmp/orders.arrow.writing"

    monkeypatch.setattr(fsspec_core, "url_to_fs", _url_to_fs)

    frame = MagicMock(spec=pl.LazyFrame)
    backend = _PolarsCheckpointBackend(
        storage_options={
            "aws_server_side_encryption": "aws:kms",
            "aws_sse_kms_key_id": "alias/loom-temp",
        }
    )
    backend.write("orders", "s3://bucket/tmp", frame, append=False)

    frame.sink_ipc.assert_called_once()
    assert frame.sink_ipc.call_args.kwargs["storage_options"] == {
        "aws_server_side_encryption": "aws:kms",
        "aws_sse_kms_key_id": "alias/loom-temp",
    }
    assert seen_fsspec == {
        "s3_additional_kwargs": {
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "alias/loom-temp",
        }
    }
    mock_fs.rename.assert_called_once()


def test_probe_does_not_pass_encryption_to_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reads decrypt server-side, so the read path keeps its options untouched."""
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    monkeypatch.setattr(fsspec_core, "url_to_fs", lambda *_a, **_k: (mock_fs, "/bucket/tmp"))
    scanned: dict[str, object] = {}
    monkeypatch.setattr(
        pl, "scan_ipc", lambda _path, **kwargs: scanned.update(kwargs) or MagicMock()
    )

    backend = _PolarsCheckpointBackend(storage_options={"aws_server_side_encryption": "aws:kms"})
    backend.probe("orders", "s3://bucket/tmp")

    assert "storage_options" not in scanned


@pytest.mark.parametrize(
    ("base", "memory_map"),
    [
        ("s3://bucket/tmp", False),
        ("r2://bucket/tmp", False),
        ("S3://bucket/tmp", False),
        ("abfss://container@account/tmp", False),
        ("/local/tmp", True),
    ],
)
def test_probe_memory_maps_only_a_local_checkpoint(
    monkeypatch: pytest.MonkeyPatch, base: str, memory_map: bool
) -> None:
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    monkeypatch.setattr(fsspec_core, "url_to_fs", lambda *_a, **_k: (mock_fs, "/bucket/tmp"))
    scanned: dict[str, object] = {}
    monkeypatch.setattr(
        pl, "scan_ipc", lambda _path, **kwargs: scanned.update(kwargs) or MagicMock()
    )

    _PolarsCheckpointBackend(storage_options={}).probe("orders", base)

    assert scanned["memory_map"] is memory_map


@pytest.mark.parametrize(
    ("declared", "object_store_value", "botocore_value"),
    [("false", "false", False), ("yes", "true", True)],
)
def test_write_passes_the_same_bucket_key_flag_to_both_halves(
    monkeypatch: pytest.MonkeyPatch,
    declared: str,
    object_store_value: str,
    botocore_value: bool,
) -> None:
    """object_store takes the flag as a string, botocore as a bool, same value."""
    seen_fsspec: dict[str, object] = {}

    def _url_to_fs(_path: str, **kwargs: object) -> tuple[object, str]:
        seen_fsspec.update(kwargs)
        return MagicMock(), "/bucket/tmp/orders.arrow.writing"

    monkeypatch.setattr(fsspec_core, "url_to_fs", _url_to_fs)

    frame = MagicMock(spec=pl.LazyFrame)
    backend = _PolarsCheckpointBackend(storage_options={"aws_sse_bucket_key_enabled": declared})
    backend.write("orders", "s3://bucket/tmp", frame, append=False)

    sunk = frame.sink_ipc.call_args.kwargs["storage_options"]
    assert sunk["aws_sse_bucket_key_enabled"] == object_store_value
    assert seen_fsspec["s3_additional_kwargs"]["BucketKeyEnabled"] is botocore_value
