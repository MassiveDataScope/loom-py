"""Unit tests for cloud checkpoint cleaners."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest

from loom.etl.checkpoint._cleaners import FsspecTempCleaner
from loom.etl.checkpoint._options import checkpoint_options
from loom.etl.event import EtlEvent, EtlKey
from tests.helpers.loggers import CapturedLog


class TestCheckpointCleaner:
    def test_delete_tree_removes_an_existing_tree_and_keeps_the_sibling(self, memfs: Any) -> None:
        memfs.pipe("bucket/tmp/part-0.arrow", b"0")
        memfs.pipe("bucket/tmp/sub/part-1.arrow", b"1")
        memfs.pipe("bucket/keep.arrow", b"keep")

        FsspecTempCleaner().delete_tree("memory://bucket/tmp")

        assert not memfs.exists("bucket/tmp")
        assert memfs.exists("bucket/keep.arrow")

    def test_delete_tree_leaves_the_store_unchanged_when_the_path_is_absent(
        self, memfs: Any
    ) -> None:
        memfs.pipe("bucket/keep.arrow", b"keep")

        FsspecTempCleaner().delete_tree("memory://bucket/tmp")

        assert memfs.find("bucket") == ["/bucket/keep.arrow"]

    def test_delete_tree_logs_a_warning_instead_of_raising_when_cleanup_fails(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.WARNING, logger="loom.etl.checkpoint._cleaners")

        FsspecTempCleaner().delete_tree("nosuchproto://bucket/tmp")

        assert CapturedLog(caplog, "loom.etl.checkpoint._cleaners").messages == [
            EtlEvent.CHECKPOINT_CLEANUP_SKIPPED
        ]

    # ---------------------------------------------------------------------------
    # CheckpointStore integration — cleaner is called on cleanup
    # ---------------------------------------------------------------------------

    def test_checkpoint_store_cleanup_run(self, tmp_path: Path) -> None:
        from loom.etl.checkpoint import CheckpointStore

        store = CheckpointStore(root=str(tmp_path))
        run_dir = tmp_path / "runs" / "run-abc"
        run_dir.mkdir(parents=True)

        store.cleanup_run("run-abc")

        assert not run_dir.exists()


class TestCheckpointStorageOptions:
    # ---------------------------------------------------------------------------
    # Storage options filter — server-side encryption survives it
    # ---------------------------------------------------------------------------

    def test_storage_options_keep_encryption_without_endpoint(self) -> None:
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

    def test_storage_options_keep_encryption_with_endpoint(self) -> None:
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

    def test_storage_options_accept_uppercase_encryption_keys(self) -> None:
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

    def test_storage_options_drop_unsupported_keys(self, caplog: pytest.LogCaptureFixture) -> None:
        log_ctx = caplog.at_level(logging.DEBUG, logger="loom.etl.checkpoint._options")
        with log_ctx:
            options = checkpoint_options({"aws_region": "eu-west-1", "aws_allow_http": "true"})

        assert options.fsspec == {}
        assert options.object_store == {}
        [fields] = CapturedLog(caplog).named(EtlEvent.CHECKPOINT_OPTIONS_IGNORED)
        assert fields[EtlKey.OPTION_KEYS] == ["aws_allow_http", "aws_region"]

    @pytest.mark.parametrize(
        ("declared", "object_store_value", "botocore_value"),
        [("false", "false", False), ("yes", "true", True)],
    )
    def test_bucket_key_enabled_is_parsed_once_for_both_forms(
        self, declared: str, object_store_value: str, botocore_value: bool
    ) -> None:
        """Both halves of the atomic write must read the same flag."""
        options = checkpoint_options({"aws_sse_bucket_key_enabled": declared})

        assert options.object_store["aws_sse_bucket_key_enabled"] == object_store_value
        assert options.fsspec["s3_additional_kwargs"]["BucketKeyEnabled"] is botocore_value
