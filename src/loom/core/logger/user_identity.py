"""What a security event writes about the user it concerns."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass

from loom.core.logger.field import LogField
from loom.core.logger.settings import LogSettings, UserIdentityMode
from loom.core.model.redaction import Secret


@dataclass(frozen=True, slots=True)
class UserIdentityPolicy:
    """The one owner of how a subject appears in a log record.

    Built once from ``observability.log`` and reached through
    :attr:`~loom.core.observability.Observability.user_identity` by the code that
    emits a security event. Operational events do not ask it: they carry no user.

    Attributes:
        mode: How the subject is written.
        hash_key: Key of the ``hash`` mode.
    """

    mode: UserIdentityMode = UserIdentityMode.NONE
    hash_key: Secret[str] | None = None

    @classmethod
    def from_settings(cls, settings: LogSettings) -> UserIdentityPolicy:
        """The policy ``observability.log`` declares."""
        return cls(settings.user_identity, settings.user_hash_key)

    def fields_for(self, subject: str) -> dict[str, str]:
        """The identity fields of a record about *subject*.

        Args:
            subject: The authenticated subject id.

        Returns:
            ``user.hash`` (HMAC-SHA256 of the subject under the key, in hex) for
            ``hash``, ``user.id`` for ``id``, and nothing for ``none``.
        """
        match self.mode:
            case UserIdentityMode.HASH:
                return {LogField.USER_HASH: self._digest_of(subject)}
            case UserIdentityMode.ID:
                return {LogField.USER_ID: subject}
            case UserIdentityMode.NONE:
                return {}

    def _digest_of(self, subject: str) -> str:
        if self.hash_key is None:
            raise ValueError("user_identity 'hash' requires user_hash_key")
        key = self.hash_key.reveal().encode()
        return hmac.new(key, subject.encode(), hashlib.sha256).hexdigest()
