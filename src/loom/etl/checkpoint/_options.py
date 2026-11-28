"""Translation of declared checkpoint storage options into engine forms."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, NamedTuple

from loom.etl.event import EtlEvent, EtlKey

_log = logging.getLogger(__name__)

_ENCRYPTION_KEYS = (
    "aws_server_side_encryption",
    "aws_sse_kms_key_id",
    "aws_sse_bucket_key_enabled",
)

_BOOLEAN_KEYS = frozenset({"aws_sse_bucket_key_enabled"})

_S3_ENCRYPTION_PARAMS = {
    "aws_server_side_encryption": "ServerSideEncryption",
    "aws_sse_kms_key_id": "SSEKMSKeyId",
    "aws_sse_bucket_key_enabled": "BucketKeyEnabled",
}

_TRUTHY = frozenset({"1", "true", "yes", "on"})


class CheckpointOptions(NamedTuple):
    """The declared options in every form the checkpoint I/O needs.

    Attributes:
        fsspec: Options for ``fsspec`` — the cleaner, the existence probe and the
            rename half of the atomic write.  Encryption travels here as the
            ``s3_additional_kwargs`` botocore envelope.
        object_store: Options for Polars' ``sink_ipc``, backed by Rust
            ``object_store``, which takes the same values under ``aws_*`` names.
        object_store_read: ``object_store`` options for ``scan_ipc``, without
            encryption: decryption is server-side, so a read declares nothing.
    """

    fsspec: dict[str, Any]
    object_store: dict[str, str]
    object_store_read: dict[str, str]


def encryption_options(storage_options: Mapping[str, str]) -> dict[str, str]:
    """Return the declared server-side encryption options, lowercase-keyed.

    Both the lowercase (delta-rs/object_store) and uppercase (environment
    variable) spellings are accepted, as for endpoint and credentials.  A
    boolean-valued option is normalised to ``"true"``/``"false"`` here, once, so
    that every consumer derives its own spelling from the same parse.
    """
    found: dict[str, str] = {}
    for name in _ENCRYPTION_KEYS:
        value = storage_options.get(name) or storage_options.get(name.upper(), "")
        if not value:
            continue
        found[name] = _as_flag(value) if name in _BOOLEAN_KEYS else value
    return found


def checkpoint_options(storage_options: Mapping[str, str]) -> CheckpointOptions:
    """Translate a delta-rs style mapping into the forms the checkpoint I/O takes.

    Only the options both engines understand survive: an S3-compatible endpoint
    with its credentials, and the server-side encryption parameters.  Explicit
    credentials are forwarded only alongside an endpoint, so that a plain AWS
    target deliberately keeps using the ambient credential chain.  Any other
    entry is dropped, and logged at debug.
    """
    fsspec_opts: dict[str, Any] = {}
    object_store_opts: dict[str, str] = {}
    consumed = {
        "endpoint_url",
        "AWS_ENDPOINT_URL",
        "access_key_id",
        "AWS_ACCESS_KEY_ID",
        "secret_access_key",
        "AWS_SECRET_ACCESS_KEY",
    }

    endpoint = storage_options.get("endpoint_url") or storage_options.get("AWS_ENDPOINT_URL", "")
    if endpoint:
        fsspec_opts["endpoint_url"] = endpoint
        object_store_opts["aws_endpoint_url"] = endpoint
        key = storage_options.get("access_key_id") or storage_options.get("AWS_ACCESS_KEY_ID", "")
        secret = storage_options.get("secret_access_key") or storage_options.get(
            "AWS_SECRET_ACCESS_KEY", ""
        )
        if key:
            fsspec_opts["key"] = key
            object_store_opts["aws_access_key_id"] = key
        if secret:
            fsspec_opts["secret"] = secret
            object_store_opts["aws_secret_access_key"] = secret

    read_opts = dict(object_store_opts)

    encryption = encryption_options(storage_options)
    if encryption:
        fsspec_opts["s3_additional_kwargs"] = _s3_additional_kwargs(encryption)
        object_store_opts |= encryption
    consumed |= set(_ENCRYPTION_KEYS) | {name.upper() for name in _ENCRYPTION_KEYS}

    dropped = sorted(set(storage_options) - consumed)
    if dropped:
        _log.debug(EtlEvent.CHECKPOINT_OPTIONS_IGNORED, extra={EtlKey.OPTION_KEYS: dropped})
    return CheckpointOptions(fsspec_opts, object_store_opts, read_opts)


def _s3_additional_kwargs(encryption: Mapping[str, str]) -> dict[str, Any]:
    """Translate normalised encryption options into botocore parameters.

    fsspec/s3fs forwards ``s3_additional_kwargs`` verbatim to botocore, where
    ``BucketKeyEnabled`` is a boolean shape and would be refused as a string.
    """
    params: dict[str, Any] = {}
    for name, param in _S3_ENCRYPTION_PARAMS.items():
        value = encryption.get(name)
        if not value:
            continue
        params[param] = value == "true" if name in _BOOLEAN_KEYS else value
    return params


def _as_flag(value: str) -> str:
    return "true" if value.strip().lower() in _TRUTHY else "false"
