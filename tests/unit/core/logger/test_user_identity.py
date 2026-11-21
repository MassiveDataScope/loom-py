from __future__ import annotations

import hashlib
import hmac

import pytest

from loom.core.logger import LogField
from loom.core.logger.settings import LogSettings, UserIdentityMode
from loom.core.logger.user_identity import UserIdentityPolicy
from loom.core.model.redaction import Secret

_SUBJECT = "user-42"


def _policy(mode: UserIdentityMode, key: str | None = None) -> UserIdentityPolicy:
    settings = LogSettings(user_identity=mode, user_hash_key=None if key is None else Secret(key))
    return UserIdentityPolicy.from_settings(settings)


class TestFieldsFor:
    def test_none_writes_nothing(self) -> None:
        assert _policy(UserIdentityMode.NONE).fields_for(_SUBJECT) == {}

    def test_id_writes_the_subject_as_it_is(self) -> None:
        assert _policy(UserIdentityMode.ID).fields_for(_SUBJECT) == {LogField.USER_ID: _SUBJECT}

    def test_hash_writes_the_hmac_sha256_of_the_subject_under_the_key(self) -> None:
        expected = hmac.new(b"k1", _SUBJECT.encode(), hashlib.sha256).hexdigest()

        assert _policy(UserIdentityMode.HASH, "k1").fields_for(_SUBJECT) == {
            LogField.USER_HASH: expected
        }

    def test_hash_never_writes_the_subject(self) -> None:
        fields = _policy(UserIdentityMode.HASH, "k1").fields_for(_SUBJECT)

        assert _SUBJECT not in fields.values()

    def test_the_same_subject_hashes_alike_and_a_key_change_changes_it(self) -> None:
        first = _policy(UserIdentityMode.HASH, "k1")
        other_key = _policy(UserIdentityMode.HASH, "k2")

        assert first.fields_for(_SUBJECT) == first.fields_for(_SUBJECT)
        assert first.fields_for(_SUBJECT) != other_key.fields_for(_SUBJECT)

    def test_the_default_policy_writes_nothing(self) -> None:
        assert UserIdentityPolicy().fields_for(_SUBJECT) == {}


class TestKeyNeverShows:
    def test_the_policy_does_not_print_its_key(self) -> None:
        assert "k1" not in repr(_policy(UserIdentityMode.HASH, "k1"))

    def test_a_policy_built_by_hand_with_no_key_cannot_hash(self) -> None:
        with pytest.raises(ValueError, match="requires user_hash_key"):
            UserIdentityPolicy(UserIdentityMode.HASH).fields_for(_SUBJECT)
