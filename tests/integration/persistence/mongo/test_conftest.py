"""The skip message names the server without its credentials."""

from __future__ import annotations

import pytest

from .conftest import redact_uri


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        (
            "mongodb://user:s3cret@db.example:27017/?replicaSet=rs0",
            "mongodb://user:***@db.example:27017/?replicaSet=rs0",
        ),
        (
            "mongodb+srv://user:p%40ss@cluster.example/shop",
            "mongodb+srv://user:***@cluster.example/shop",
        ),
        ("mongodb://localhost:27017/?replicaSet=rs0", "mongodb://localhost:27017/?replicaSet=rs0"),
    ],
)
def test_redact_uri_masks_the_password(uri: str, expected: str) -> None:
    assert redact_uri(uri) == expected
