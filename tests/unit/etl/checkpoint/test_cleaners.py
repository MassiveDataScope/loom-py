"""Unit tests for cloud checkpoint cleaners."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import fsspec.core as fsspec_core
import pytest

from loom.etl.checkpoint._cleaners import FsspecTempCleaner
from loom.etl.checkpoint._options import checkpoint_options


def test_cleaner_calls_rm_when_path_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    monkeypatch.setattr(
        fsspec_core,
        "url_to_fs",
        lambda *_args, **_kwargs: (mock_fs, "/bucket/tmp"),
    )

    FsspecTempCleaner().delete_tree("s3://bucket/tmp")

    mock_fs.rm.assert_called_once_with("/bucket/tmp", recursive=True)


def test_cleaner_skips_rm_when_path_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_fs = MagicMock()
    mock_fs.exists.return_value = False
    monkeypatch.setattr(
        fsspec_core,
        "url_to_fs",
        lambda *_args, **_kwargs: (mock_fs, "/bucket/tmp"),
    )
    FsspecTempCleaner().delete_tree("s3://bucket/tmp")

    mock_fs.rm.assert_not_called()


def test_cleaner_logs_warning_on_exception(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(*_args: object, **_kwargs: object) -> tuple[object, str]:
        raise PermissionError("no credentials")

    monkeypatch.setattr(fsspec_core, "url_to_fs", _raise)

    import logging

    log_ctx = caplog.at_level(logging.WARNING, logger="loom.etl.checkpoint._cleaners")
    with log_ctx:
        FsspecTempCleaner().delete_tree("s3://bucket/tmp")

    assert any("cleanup skipped" in r.message for r in caplog.records)


def test_cleaner_does_not_raise_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_args: object, **_kwargs: object) -> tuple[object, str]:
        raise RuntimeError("boom")

    monkeypatch.setattr(fsspec_core, "url_to_fs", _raise)
    FsspecTempCleaner().delete_tree("s3://bucket/tmp")  # must not raise


# ---------------------------------------------------------------------------
# CheckpointStore integration — cleaner is called on cleanup
# ---------------------------------------------------------------------------


def test_checkpoint_store_cleanup_run(tmp_path: Path) -> None:
    from loom.etl.checkpoint import CheckpointStore

    store = CheckpointStore(root=str(tmp_path))
    run_dir = tmp_path / "runs" / "run-abc"
    run_dir.mkdir(parents=True)

    store.cleanup_run("run-abc")

    assert not run_dir.exists()


# ---------------------------------------------------------------------------
# Storage options filter — server-side encryption survives it
# ---------------------------------------------------------------------------


def test_storage_options_keep_encryption_without_endpoint() -> None:
    options = checkpoint_options(
        {
            "aws_server_side_encryption": "aws:kms",
            "aws_sse_kms_key_id": "alias/loom-temp",
            "aws_sse_bucket_key_enabled": "true",
        }
    )

    assert options.fsspec == {
        "s3_additional_kwargs": {
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "alias/loom-temp",
            "BucketKeyEnabled": True,
        }
    }
    assert options.object_store == {
        "aws_server_side_encryption": "aws:kms",
        "aws_sse_kms_key_id": "alias/loom-temp",
        "aws_sse_bucket_key_enabled": "true",
    }


def test_storage_options_keep_encryption_with_endpoint() -> None:
    options = checkpoint_options(
        {
            "endpoint_url": "http://minio:9000",
            "access_key_id": "ak",
            "secret_access_key": "sk",
            "aws_server_side_encryption": "aws:kms",
        }
    )

    assert options.fsspec == {
        "endpoint_url": "http://minio:9000",
        "key": "ak",
        "secret": "sk",
        "s3_additional_kwargs": {"ServerSideEncryption": "aws:kms"},
    }
    assert options.object_store == {
        "aws_endpoint_url": "http://minio:9000",
        "aws_access_key_id": "ak",
        "aws_secret_access_key": "sk",
        "aws_server_side_encryption": "aws:kms",
    }
    assert options.object_store_read == {
        "aws_endpoint_url": "http://minio:9000",
        "aws_access_key_id": "ak",
        "aws_secret_access_key": "sk",
    }


def test_storage_options_accept_uppercase_encryption_keys() -> None:
    options = checkpoint_options(
        {
            "AWS_SERVER_SIDE_ENCRYPTION": "aws:kms",
            "AWS_SSE_KMS_KEY_ID": "alias/loom-temp",
        }
    )

    assert options.fsspec["s3_additional_kwargs"] == {
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": "alias/loom-temp",
    }


def test_storage_options_drop_unsupported_keys(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    log_ctx = caplog.at_level(logging.DEBUG, logger="loom.etl.checkpoint._options")
    with log_ctx:
        options = checkpoint_options({"aws_region": "eu-west-1", "aws_allow_http": "true"})

    assert options.fsspec == {}
    assert options.object_store == {}
    assert any("storage options ignored" in r.message for r in caplog.records)


@pytest.mark.parametrize(
    ("declared", "object_store_value", "botocore_value"),
    [("false", "false", False), ("yes", "true", True)],
)
def test_bucket_key_enabled_is_parsed_once_for_both_forms(
    declared: str, object_store_value: str, botocore_value: bool
) -> None:
    """Both halves of the atomic write must read the same flag."""
    options = checkpoint_options({"aws_sse_bucket_key_enabled": declared})

    assert options.object_store["aws_sse_bucket_key_enabled"] == object_store_value
    assert options.fsspec["s3_additional_kwargs"]["BucketKeyEnabled"] is botocore_value
