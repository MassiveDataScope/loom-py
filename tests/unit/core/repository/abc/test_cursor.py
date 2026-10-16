from __future__ import annotations

import base64
import json
import string
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from loom.core.errors.codes import ErrorCode
from loom.core.repository.abc import UnsupportedQuery
from loom.core.repository.abc.cursor import Cursor, decode_cursor, encode_cursor

_BACKEND = "sqlalchemy"
_MODEL = "Product"


class TestRoundTrip:
    def test_scalar_keys_and_tie_breaker_survive(self) -> None:
        token = encode_cursor(_BACKEND, [20.0, "b", True, None, 7], 42)

        cursor = decode_cursor(token, _BACKEND, _MODEL)

        assert cursor == Cursor(backend=_BACKEND, keys=(20.0, "b", True, None, 7), tie_breaker=42)

    def test_temporal_uuid_and_decimal_keys_keep_their_types(self) -> None:
        aware = datetime(2026, 9, 6, 12, 30, tzinfo=UTC)
        naive = datetime(2026, 9, 6, 12, 30)
        day = date(2026, 9, 6)
        ident = UUID("b7cdfa6d-0805-4c13-8071-af9317c08254")
        amount = Decimal("19.90")

        cursor = decode_cursor(
            encode_cursor(_BACKEND, [aware, naive, day, amount], ident), _BACKEND, _MODEL
        )

        assert cursor.keys == (aware, naive, day, amount)
        assert cursor.tie_breaker == ident
        assert isinstance(cursor.keys[3], Decimal)
        assert isinstance(cursor.tie_breaker, UUID)

    def test_token_is_url_safe(self) -> None:
        token = encode_cursor(_BACKEND, ["a/b+c"], 1)

        assert set(token) <= set(string.ascii_letters + string.digits + "-_=")


class TestCompositeTieBreaker:
    def test_every_key_value_survives(self) -> None:
        token = encode_cursor(_BACKEND, ["b"], (7, 3))

        cursor = decode_cursor(token, _BACKEND, _MODEL, id_count=2)

        assert cursor == Cursor(backend=_BACKEND, keys=("b",), tie_breaker=(7, 3))

    def test_single_key_token_keeps_a_scalar_id(self) -> None:
        token = encode_cursor(_BACKEND, [], 42)

        assert json.loads(base64.urlsafe_b64decode(token))["id"] == 42

    @pytest.mark.parametrize(
        ("tie_breaker", "id_count"), [(7, 2), ((7,), 1), ((7, 3), 1), ((7, 3, 1), 2)]
    )
    def test_key_value_count_must_match_the_primary_key(
        self, tie_breaker: object, id_count: int
    ) -> None:
        token = encode_cursor(_BACKEND, [], tie_breaker)

        with pytest.raises(UnsupportedQuery) as excinfo:
            decode_cursor(token, _BACKEND, _MODEL, id_count=id_count)

        assert excinfo.value.reason == "cursor token does not match the primary key"


class TestRejectedTokens:
    def test_token_from_another_backend_is_unsupported(self) -> None:
        """The reason is fixed: the client-controlled backend name is never echoed."""
        token = encode_cursor("<script>alert(1)</script>", [1], 1)

        with pytest.raises(UnsupportedQuery) as excinfo:
            decode_cursor(token, _BACKEND, _MODEL)

        assert excinfo.value.backend == _BACKEND
        assert excinfo.value.model == _MODEL
        assert excinfo.value.code == ErrorCode.UNSUPPORTED_QUERY
        assert excinfo.value.reason == "cursor token was issued by another backend"

    def test_old_format_token_is_unsupported(self) -> None:
        token = base64.urlsafe_b64encode(json.dumps({"id": 42}).encode()).decode()

        with pytest.raises(UnsupportedQuery) as excinfo:
            decode_cursor(token, _BACKEND, _MODEL)

        assert excinfo.value.code == ErrorCode.UNSUPPORTED_QUERY

    @pytest.mark.parametrize("token", ["", "not base64!", "Zm9v", "eyJiIjo="])
    def test_garbage_token_is_unsupported(self, token: str) -> None:
        with pytest.raises(UnsupportedQuery):
            decode_cursor(token, _BACKEND, _MODEL)
