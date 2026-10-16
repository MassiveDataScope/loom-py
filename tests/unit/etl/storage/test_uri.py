"""is_cloud_path tells a cloud URI from a local path."""

from __future__ import annotations

import pytest

from loom.etl.storage._uri import is_cloud_path


@pytest.mark.parametrize(
    "path", ["s3://bucket/key", "GS://bucket", "abfss://container@account/path", "r2://b/k"]
)
def test_a_supported_scheme_is_a_cloud_path(path: str) -> None:
    assert is_cloud_path(path)


@pytest.mark.parametrize(
    "path", ["/srv/etl/config.yaml", "config.yaml", "file:///tmp/x", "ftp://h/x"]
)
def test_a_local_path_or_another_scheme_is_not(path: str) -> None:
    assert not is_cloud_path(path)
