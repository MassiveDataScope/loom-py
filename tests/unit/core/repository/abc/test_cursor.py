from __future__ import annotations

import base64
import secrets
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

import msgspec
import pytest

from loom.core.config import ConfigError
from loom.core.errors.codes import ErrorCode
from loom.core.repository.abc import (
    Cursor,
    CursorCodec,
    FilterGroup,
    FilterOp,
    FilterSpec,
    PaginationMode,
    QuerySpec,
    SortSpec,
    UnsupportedQuery,
)

_BACKEND = "sqlalchemy"
_MODEL = "billing.Invoice"
_SCOPE = "tenant-a"
_INVALID = "cursor token is not valid"
_QUERY = QuerySpec(
    sort=(SortSpec(field="created_at", direction="DESC"),),
    pagination=PaginationMode.CURSOR,
)
# Columns of any type, for the tests about the token rather than its values.
_UNTYPED: dict[str, Any] = {"created_at": Any, "updated_at": Any, "account_id": Any, "id": Any}


def _key() -> bytes:
    return secrets.token_bytes(32)


def _signed(**keys: bytes) -> CursorCodec:
    return CursorCodec(signing="signed", keys=keys, current_key_id=next(iter(keys)))


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _decode(
    codec: CursorCodec,
    token: str,
    *,
    query: QuerySpec = _QUERY,
    scope: str = _SCOPE,
    columns: dict[str, Any] = _UNTYPED,
    primary_key: tuple[str, ...] = ("id",),
) -> Cursor:
    return codec.decode(
        token,
        backend=_BACKEND,
        model=_MODEL,
        query=query,
        scope=scope,
        columns=columns,
        primary_key=primary_key,
    )


def _encode(
    codec: CursorCodec, cursor: Cursor, *, query: QuerySpec = _QUERY, scope: str = _SCOPE
) -> str:
    return codec.encode(cursor, model=_MODEL, query=query, scope=scope)


def _at(*keys: object, tie_breaker: object = 1, backend: str = _BACKEND) -> Cursor:
    return Cursor(backend=backend, keys=keys, tie_breaker=tie_breaker)


def _filtered(value: object) -> QuerySpec:
    return QuerySpec(
        filters=FilterGroup(filters=(FilterSpec("status", FilterOp.EQ, value),)),
        sort=_QUERY.sort,
        pagination=PaginationMode.CURSOR,
    )


@pytest.fixture
def codec() -> CursorCodec:
    return _signed(current=_key())


@pytest.fixture
def unsigned() -> CursorCodec:
    return CursorCodec(signing="unsigned")


class TestRoundTrip:
    @pytest.mark.parametrize(
        "key",
        [
            datetime(2026, 9, 6, 12, 30, tzinfo=UTC),
            UUID("b7cdfa6d-0805-4c13-8071-af9317c08254"),
            "a/b+c",
            7,
            20.5,
            True,
            None,
            date(2026, 9, 6),
            Decimal("19.90"),
        ],
    )
    def test_every_supported_key_type_comes_back_with_its_type(
        self, codec: CursorCodec, key: object
    ) -> None:
        cursor = _decode(codec, _encode(codec, _at(key, tie_breaker=key)))

        assert cursor == _at(key, tie_breaker=key)
        assert type(cursor.keys[0]) is type(key)
        assert type(cursor.tie_breaker) is type(key)

    def test_a_composite_tie_breaker_keeps_every_value(self, codec: CursorCodec) -> None:
        position = _at("b", tie_breaker=(7, uuid4()))

        assert (
            _decode(codec, _encode(codec, position), primary_key=("account_id", "id")) == position
        )

    def test_a_naive_datetime_is_carried_as_utc(self, codec: CursorCodec) -> None:
        cursor = _decode(codec, _encode(codec, _at(datetime(2026, 9, 6, 12, 30))))

        assert cursor.keys == (datetime(2026, 9, 6, 12, 30, tzinfo=UTC),)

    def test_an_aware_datetime_keeps_its_instant(self, codec: CursorCodec) -> None:
        madrid = datetime(2026, 9, 6, 14, 30, tzinfo=timezone(timedelta(hours=2)))

        cursor = _decode(codec, _encode(codec, _at(madrid)))

        assert cursor.keys == (madrid,)

    def test_the_token_is_unpadded_base64url(self, codec: CursorCodec) -> None:
        token = _encode(codec, _at("a/b+c?" * 7))

        payload, tag = token.split(".")
        assert set(payload + tag) <= set(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        )


class TestStrictDecoding:
    """Layer 1 holds in every mode; the unsigned codec lets a test hand-craft payloads."""

    @staticmethod
    def _token(payload: object) -> str:
        return _b64(msgspec.json.encode(payload))

    def test_a_naive_datetime_key_is_refused(self, unsigned: CursorCodec) -> None:
        token = self._token(
            {"b": _BACKEND, "k": [{"type": "datetime", "v": "2026-09-06T12:30:00"}], "id": 1}
        )

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(unsigned, token)

        assert excinfo.value.reason == _INVALID

    @pytest.mark.parametrize(
        "payload",
        [
            {"b": _BACKEND, "k": [{"type": "money", "v": "1"}], "id": 1},
            {"b": _BACKEND, "k": [{"type": "uuid", "v": "not-a-uuid"}], "id": 1},
            {"b": _BACKEND, "k": "created_at", "id": 1},
            {"b": _BACKEND, "k": [1], "id": {"x": 1}},
            {"b": _BACKEND, "k": [1]},
            {"b": _BACKEND, "k": [1], "id": 1, "extra": True},
            [_BACKEND, [1], 1],
            {"id": 42},
        ],
        ids=["tag", "uuid", "keys", "id", "missing", "unknown", "array", "legacy"],
    )
    def test_a_payload_of_the_wrong_shape_is_refused(
        self, unsigned: CursorCodec, payload: object
    ) -> None:
        token = self._token(payload)

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(unsigned, token)

        assert excinfo.value.reason == _INVALID

    @pytest.mark.parametrize(
        "token",
        ["", "not base64!", "Zm9v", "eyJiIjo", "e30=", "A", "+/+/", "e30.e30"],
        ids=["empty", "alphabet", "json", "truncated", "padding", "length", "std", "dotted"],
    )
    def test_undecodable_text_is_refused(self, unsigned: CursorCodec, token: str) -> None:
        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(unsigned, token)

        assert excinfo.value.code == ErrorCode.UNSUPPORTED_QUERY

    def test_a_token_from_another_backend_is_refused(self, unsigned: CursorCodec) -> None:
        token = _encode(unsigned, _at(1, backend="mongo"))

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(unsigned, token)

        assert excinfo.value.backend == _BACKEND
        assert excinfo.value.model == _MODEL
        assert excinfo.value.reason == "cursor token was issued by another backend"

    def test_a_token_for_another_sort_is_refused(self, unsigned: CursorCodec) -> None:
        token = _encode(unsigned, _at(1, "a"))

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(unsigned, token)

        assert excinfo.value.reason == "cursor token does not match the sort"

    @pytest.mark.parametrize(
        ("tie_breaker", "primary_key"),
        [
            (7, ("account_id", "id")),
            ((7,), ("id",)),
            ((7, 3), ("id",)),
            ((7, 3, 1), ("account_id", "id")),
        ],
    )
    def test_a_token_for_another_primary_key_is_refused(
        self, unsigned: CursorCodec, tie_breaker: object, primary_key: tuple[str, ...]
    ) -> None:
        token = _encode(unsigned, _at(1, tie_breaker=tie_breaker))

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(unsigned, token, primary_key=primary_key)

        assert excinfo.value.reason == "cursor token does not match the primary key"

    def test_a_signed_codec_applies_the_same_shape_checks(self) -> None:
        key = _key()
        issuer = _signed(current=key)
        token = _encode(issuer, _at(1, "a"))

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(_signed(current=key), token)

        assert excinfo.value.reason == "cursor token does not match the sort"


class _Status(StrEnum):
    OPEN = "open"
    CLOSED = "closed"


_TYPED: dict[str, Any] = {
    "created_at": datetime,
    "closed_at": datetime | None,
    "rank": int,
    "code": str,
    "status": _Status,
    "amount": Decimal,
    "ref": UUID,
    "account_id": int,
    "id": int,
}


def _sorted_by(field: str) -> QuerySpec:
    return QuerySpec(sort=(SortSpec(field),), pagination=PaginationMode.CURSOR)


class TestTypedValues:
    """A forged token holds values the server never issued; each must have its column's type."""

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("rank", "7"),
            ("rank", True),
            ("rank", None),
            ("code", 7),
            ("created_at", "2026-09-06T12:30:00+00:00"),
            ("ref", "b7cdfa6d-0805-4c13-8071-af9317c08254"),
            ("amount", 19.9),
            ("status", "archived"),
        ],
        ids=["str-int", "bool-int", "null", "int-str", "str-dt", "str-uuid", "float-dec", "enum"],
    )
    def test_a_sort_value_of_another_type_is_refused(
        self, unsigned: CursorCodec, field: str, value: object
    ) -> None:
        query = _sorted_by(field)
        token = _encode(unsigned, _at(value), query=query)

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(unsigned, token, query=query, columns=_TYPED)

        assert excinfo.value.reason == "cursor token does not match the sort"

    @pytest.mark.parametrize(
        ("tie_breaker", "primary_key"),
        [("9", ("id",)), ((7, "9"), ("account_id", "id")), (None, ("id",))],
        ids=["str-id", "composite", "null"],
    )
    def test_a_primary_key_value_of_another_type_is_refused(
        self, unsigned: CursorCodec, tie_breaker: object, primary_key: tuple[str, ...]
    ) -> None:
        query = _sorted_by("rank")
        token = _encode(unsigned, _at(3, tie_breaker=tie_breaker), query=query)

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(unsigned, token, query=query, columns=_TYPED, primary_key=primary_key)

        assert excinfo.value.reason == "cursor token does not match the primary key"

    def test_a_validly_signed_token_of_another_type_is_refused(self, codec: CursorCodec) -> None:
        query = _sorted_by("rank")
        token = _encode(codec, _at("7"), query=query)

        with pytest.raises(UnsupportedQuery):
            _decode(codec, token, query=query, columns=_TYPED)

    def test_a_sort_field_without_a_known_type_is_refused(self, unsigned: CursorCodec) -> None:
        query = _sorted_by("category.name")
        token = _encode(unsigned, _at("a"), query=query)

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(unsigned, token, query=query, columns=_TYPED)

        assert excinfo.value.reason == "cursor token does not match the sort"

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("created_at", datetime(2026, 9, 6, 12, 30, tzinfo=UTC)),
            ("closed_at", None),
            ("rank", 7),
            ("code", "0123"),
            ("status", _Status.CLOSED),
            ("amount", Decimal("19.90")),
            ("ref", UUID("b7cdfa6d-0805-4c13-8071-af9317c08254")),
        ],
    )
    def test_a_value_of_its_column_type_comes_back_as_issued(
        self, codec: CursorCodec, field: str, value: object
    ) -> None:
        query = _sorted_by(field)

        cursor = _decode(
            codec, _encode(codec, _at(value), query=query), query=query, columns=_TYPED
        )

        assert cursor.keys == (value,)
        assert type(cursor.keys[0]) is (str if isinstance(value, StrEnum) else type(value))


class TestSignature:
    def test_a_tampered_payload_is_refused(self, codec: CursorCodec) -> None:
        payload, tag = _encode(codec, _at(1, tie_breaker=1)).split(".")
        forged = _b64(_unb64(payload).replace(b'"id":1', b'"id":2'))
        assert forged != payload

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, f"{forged}.{tag}")

        assert excinfo.value.reason == _INVALID

    def test_a_tampered_tag_is_refused(self, codec: CursorCodec) -> None:
        payload, tag = _encode(codec, _at(1)).split(".")
        raw = bytearray(_unb64(tag))
        raw[-1] ^= 0x01

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, f"{payload}.{_b64(bytes(raw))}")

        assert excinfo.value.reason == _INVALID

    def test_a_token_signed_with_another_key_is_refused(self, codec: CursorCodec) -> None:
        token = _encode(_signed(current=_key()), _at(1))

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, token)

        assert excinfo.value.reason == _INVALID

    def test_a_missing_signature_is_refused(self, codec: CursorCodec) -> None:
        payload, _ = _encode(codec, _at(1)).split(".")

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, payload)

        assert excinfo.value.reason == _INVALID

    def test_an_empty_signature_is_refused(self, codec: CursorCodec) -> None:
        payload, _ = _encode(codec, _at(1)).split(".")

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, f"{payload}.")

        assert excinfo.value.reason == _INVALID

    def test_an_unsigned_token_is_refused_by_a_signed_codec(
        self, codec: CursorCodec, unsigned: CursorCodec
    ) -> None:
        token = _encode(unsigned, _at(1))

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, token)

        assert excinfo.value.reason == _INVALID

    def test_the_refusal_never_echoes_the_token(self, codec: CursorCodec) -> None:
        marker = "<script>alert(1)</script>"
        token = _b64(msgspec.json.encode({"b": marker, "k": [marker], "id": marker}))

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, f"{token}.{token}")

        rendered = f"{excinfo.value} {excinfo.value.reason} {excinfo.value!r}"
        assert marker not in rendered
        assert token not in rendered


class TestRotation:
    def test_a_token_from_a_retired_key_still_verifies(self) -> None:
        old, new = _key(), _key()
        token = _encode(_signed(old=old), _at(1))
        rotated = CursorCodec(signing="signed", keys={"new": new, "old": old}, current_key_id="new")

        assert _decode(rotated, token) == _at(1)

    def test_a_rotated_codec_signs_with_the_current_key(self) -> None:
        old, new = _key(), _key()
        rotated = CursorCodec(signing="signed", keys={"new": new, "old": old}, current_key_id="new")
        token = _encode(rotated, _at(1))

        assert _decode(_signed(new=new), token) == _at(1)

    def test_a_token_naming_an_unknown_key_id_is_refused(self) -> None:
        key = _key()
        token = _encode(_signed(gone=key), _at(1))

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(_signed(current=key), token)

        assert excinfo.value.reason == _INVALID


class TestBinding:
    def test_a_cursor_bound_to_one_tenant_is_refused_in_another(self, codec: CursorCodec) -> None:
        token = _encode(codec, _at(1), scope="tenant-a")

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, token, scope="tenant-b")

        assert excinfo.value.reason == _INVALID

    def test_a_cursor_bound_to_one_filter_is_refused_with_another(self, codec: CursorCodec) -> None:
        token = _encode(codec, _at(1), query=_filtered("open"))

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, token, query=_filtered("closed"))

        assert excinfo.value.reason == _INVALID

    def test_a_cursor_issued_for_one_direction_is_refused_for_the_other(
        self, codec: CursorCodec
    ) -> None:
        ascending = QuerySpec(sort=(SortSpec("created_at"),), pagination=PaginationMode.CURSOR)
        token = _encode(codec, _at(1), query=ascending)

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, token)

        assert excinfo.value.reason == _INVALID

    def test_a_cursor_issued_for_one_sort_field_is_refused_for_another(
        self, codec: CursorCodec
    ) -> None:
        by_update = QuerySpec(
            sort=(SortSpec("updated_at", "DESC"),), pagination=PaginationMode.CURSOR
        )
        token = _encode(codec, _at(1), query=by_update)

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, token)

        assert excinfo.value.reason == _INVALID

    def test_a_cursor_for_one_model_is_refused_for_another(self, codec: CursorCodec) -> None:
        token = codec.encode(_at(1), model="billing.Refund", query=_QUERY, scope=_SCOPE)

        with pytest.raises(UnsupportedQuery) as excinfo:
            _decode(codec, token)

        assert excinfo.value.reason == _INVALID

    def test_the_same_instant_in_another_zone_is_the_same_filter(self, codec: CursorCodec) -> None:
        utc = datetime(2026, 9, 6, 12, 30, tzinfo=UTC)
        token = _encode(codec, _at(1), query=_filtered(utc))
        madrid = utc.astimezone(timezone(timedelta(hours=2)))

        assert _decode(codec, token, query=_filtered(madrid)) == _at(1)

    def test_the_order_of_and_filters_does_not_change_the_binding(self, codec: CursorCodec) -> None:
        first = FilterSpec("status", FilterOp.EQ, "open")
        second = FilterSpec("total", FilterOp.GT, 10)

        def query(*filters: FilterSpec) -> QuerySpec:
            return QuerySpec(
                filters=FilterGroup(filters=filters),
                sort=_QUERY.sort,
                pagination=PaginationMode.CURSOR,
            )

        token = _encode(codec, _at(1), query=query(first, second))

        assert _decode(codec, token, query=query(second, first)) == _at(1)

    def test_page_size_is_not_part_of_the_binding(self, codec: CursorCodec) -> None:
        token = _encode(codec, _at(1))
        larger = QuerySpec(sort=_QUERY.sort, pagination=PaginationMode.CURSOR, limit=200)

        assert _decode(codec, token, query=larger) == _at(1)


class TestModes:
    def test_the_unsigned_mode_accepts_an_unsigned_token(self, unsigned: CursorCodec) -> None:
        token = _b64(msgspec.json.encode({"b": _BACKEND, "k": [3], "id": 9}))

        assert _decode(unsigned, token) == _at(3, tie_breaker=9)

    def test_the_unsigned_mode_warns_when_built(self, caplog: pytest.LogCaptureFixture) -> None:
        CursorCodec(signing="unsigned")

        assert any(
            record.levelname == "WARNING" and "unsigned" in record.getMessage()
            for record in caplog.records
        )

    def test_the_ephemeral_mode_round_trips_and_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ephemeral = CursorCodec(signing="ephemeral")

        assert _decode(ephemeral, _encode(ephemeral, _at(1))) == _at(1)
        assert any(
            record.levelname == "WARNING" and "restart" in record.getMessage()
            for record in caplog.records
        )

    def test_two_ephemeral_codecs_do_not_share_a_key(self) -> None:
        token = _encode(CursorCodec(signing="ephemeral"), _at(1))

        with pytest.raises(UnsupportedQuery):
            _decode(CursorCodec(signing="ephemeral"), token)

    def test_a_short_key_is_refused(self) -> None:
        with pytest.raises(ConfigError, match="32 bytes"):
            _signed(current=secrets.token_bytes(31))

    def test_a_signed_codec_needs_its_current_key(self) -> None:
        with pytest.raises(ConfigError, match="current_key_id"):
            CursorCodec(signing="signed", keys={"old": _key()}, current_key_id="new")

    def test_a_signed_codec_without_keys_is_refused(self) -> None:
        with pytest.raises(ConfigError, match=r"persistence\.cursors\.signing: ephemeral"):
            CursorCodec(signing="signed")
