"""Who acted, in loom's security records: a keyed pseudonym of the subject, never the subject."""

from __future__ import annotations

import hashlib
import hmac
import logging
from dataclasses import dataclass
from typing import Final

from loom.core.log_names import LogEvent, LogField, LogKey
from loom.core.model.redaction import Secret

USER_HASH_LENGTH: Final = 32
"""Hex characters of a written ``user.hash``: the first 128 bits of the HMAC-SHA256."""

_log = logging.getLogger(__name__)


class LoggerEvent(LogEvent):
    """What the logging setup logs about itself."""

    SUBJECT_HASH_DISABLED = "logging.subject_hash_disabled"


@dataclass(frozen=True, slots=True)
class SubjectHasher:
    """The one owner of how a security record names its caller: ``observability.log.subject_key``.

    A subject is written as the HMAC-SHA256 of its UTF-8 bytes under the key, in
    hex, cut to :data:`USER_HASH_LENGTH` characters. Whoever holds the key finds a
    subject's records by computing the same HMAC; nobody else can reverse it or
    test a guess. Without a key nothing is written: never the subject, never an
    unkeyed hash.

    Attributes:
        secret: The key the pseudonyms are keyed with; ``None`` writes none.
    """

    secret: Secret[str] | None = None

    @classmethod
    def installed(cls) -> SubjectHasher:
        """The hasher of the process: the last one installed, keyless before any."""
        return _installed or _KEYLESS

    def install(self) -> None:
        """Make this the hasher of the process.

        Installing a keyless hasher logs ``logging.subject_hash_disabled`` once:
        not again while keyless ones replace each other.
        """
        global _installed
        was_keyless = _installed is not None and _installed.secret is None
        _installed = self
        if self.secret is None and not was_keyless:
            _log.warning(LoggerEvent.SUBJECT_HASH_DISABLED)

    def fields(self, subject: str | None, key: LogKey = LogField.USER_HASH) -> dict[str, str]:
        """The field naming *subject* under *key*; none without a secret or a subject.

        A subject that is not valid UTF-8, a lone surrogate, is hashed as it is.
        """
        if self.secret is None or not subject:
            return {}
        digest = hmac.new(
            self.secret.reveal().encode(), subject.encode("utf-8", "surrogatepass"), hashlib.sha256
        )
        return {key: digest.hexdigest()[:USER_HASH_LENGTH]}


_KEYLESS: Final = SubjectHasher()
_installed: SubjectHasher | None = None
