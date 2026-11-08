"""File resolver: ``${file:<path or URI>}`` becomes the text of that file.

It reads what :func:`~loom.core.config.load_config` reads, a local path or a
cloud object (``s3://``, ``gs://``, ``abfss://``, ...), with the same reader.
Typed as a :class:`~loom.core.model.redaction.Secret`, the value stays out of
reprs, logs and dumps, so a key file needs no path field of its own::

    jwt_issuer:
      signing_key: ${file:/run/secrets/jwt-signing.pem}
      algorithm: EdDSA
"""

from __future__ import annotations

from loom.core.config._includes import read_text
from loom.core.config.errors import ConfigError


class FileResolver:
    """Resolve ``${file:...}`` placeholders to the UTF-8 text of the file.

    The text is returned as stored, trailing line break included: a key is
    used byte for byte. The key is trusted deployment configuration, never
    request input.
    """

    @property
    def name(self) -> str:
        """OmegaConf resolver prefix.

        Returns:
            ``"file"``.
        """
        return "file"

    def resolve(self, key: str) -> str:
        """Read the file at *key*.

        Args:
            key: Local path or cloud URI, as written after ``file:``.

        Returns:
            The file's text.

        Raises:
            ConfigError: When the key is blank or the file cannot be read. The
                message names the error type only, never the path (it says
                where secret material lives) nor the content, and the error is
                neither chained nor kept as context: a ``UnicodeDecodeError``
                carries the file's bytes.
        """
        if not key.strip():
            raise ConfigError("file resolver key must not be empty")
        try:
            return read_text(key)
        except Exception as exc:  # noqa: BLE001 - every backend raises its own types
            failure = type(exc).__name__
        # Raised outside the handler: ``from None`` only hides the cause, and
        # ``__context__`` would still hold it, bytes included, for any logger
        # that walks it.
        raise ConfigError(f"Could not read a ${{file:...}} value: {failure}")
