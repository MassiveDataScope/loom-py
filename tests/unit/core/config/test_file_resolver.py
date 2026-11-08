"""Tests for the ``file`` resolver: ``${file:...}`` reads what ``load_config`` reads."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import fsspec
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from loom.core.config import FileResolver, load_config, section, with_default_resolvers
from loom.core.config.errors import ConfigError
from loom.core.model.redaction import Secret
from loom.rest.auth import JwtIssuerConfig


@pytest.fixture(autouse=True)
def _fresh_resolvers(clear_builtin_resolvers: None) -> Iterator[None]:
    yield


def test_a_local_file_is_read_as_stored(tmp_path: Path) -> None:
    key = tmp_path / "key.pem"
    key.write_text("material\n", encoding="utf-8")

    assert FileResolver().resolve(str(key)) == "material\n"


def test_a_cloud_object_is_read_with_the_config_reader() -> None:
    with fsspec.open("memory://loom-file-resolver/key", "w") as handle:
        handle.write("from-object-storage")

    assert FileResolver().resolve("memory://loom-file-resolver/key") == "from-object-storage"


def test_a_blank_key_is_refused() -> None:
    with pytest.raises(ConfigError, match="must not be empty"):
        FileResolver().resolve("  ")


def test_a_missing_file_names_the_key_and_chains_nothing(tmp_path: Path) -> None:
    missing = str(tmp_path / "absent.pem")

    with pytest.raises(ConfigError, match="FileNotFoundError") as refused:
        FileResolver().resolve(missing)

    assert missing in str(refused.value)
    assert refused.value.__cause__ is None
    assert refused.value.__suppress_context__


def test_an_undecodable_file_never_carries_its_bytes(tmp_path: Path) -> None:
    """A ``UnicodeDecodeError`` holds the file on ``exc.object``: it must not travel."""
    key = tmp_path / "binary.der"
    key.write_bytes(b"\x30\x82\x01\x22\xff\xfe-binary-secret")

    with pytest.raises(ConfigError, match="UnicodeDecodeError") as refused:
        FileResolver().resolve(str(key))

    assert "binary-secret" not in str(refused.value)
    assert refused.value.__cause__ is None


def test_a_key_file_reaches_the_issuer_as_a_secret(tmp_path: Path) -> None:
    """End to end: the placeholder reads the key and the config keeps it masked."""
    private = (
        ed25519.Ed25519PrivateKey.generate()
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        .decode()
    )
    key = tmp_path / "signing.pem"
    key.write_text(private, encoding="utf-8")
    config_file = tmp_path / "app.yaml"
    config_file.write_text(
        "issuer:\n"
        f"  signing_key: ${{file:{key}}}\n"
        "  algorithm: EdDSA\n"
        "  audience: api\n"
        "  issuer: gateway\n"
        "  roles_claim: roles\n",
        encoding="utf-8",
    )

    cfg = load_config(str(config_file), resolvers=with_default_resolvers())
    issuer = section(cfg, "issuer", JwtIssuerConfig)

    assert isinstance(issuer.signing_key, Secret)
    assert issuer.signing_key.reveal() == private
    assert "PRIVATE KEY" not in repr(issuer)
