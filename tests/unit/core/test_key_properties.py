"""The derived key is a function of the canonical form of its values, over generated input."""

from __future__ import annotations

import unicodedata
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from hypothesis import given
from hypothesis import strategies as st

from loom.core.model import KeyHash
from tests.helpers.keys import derived_id

UUID5 = KeyHash.uuid5(UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8"))
SHA256 = KeyHash.sha256()


_scalars = st.one_of(
    st.text(max_size=12),
    st.integers(),
    st.booleans(),
    st.decimals(allow_nan=False, allow_infinity=False, places=4),
    st.dates(),
    st.datetimes(timezones=st.timezones()),
    st.uuids(),
)


class TestKeyProperties:
    @given(values=st.lists(_scalars, min_size=1, max_size=4))
    def test_equal_values_give_equal_keys(self, values: list[object]) -> None:
        named = {f"f{i}": value for i, value in enumerate(values)}
        again = {name: _equal_copy(value) for name, value in named.items()}

        assert derived_id(UUID5, **named) == derived_id(UUID5, **again)
        assert derived_id(SHA256, **named) == derived_id(SHA256, **again)

    @given(first=st.text(max_size=12), second=st.text(max_size=12))
    def test_values_that_differ_in_their_canonical_form_give_different_keys(
        self, first: str, second: str
    ) -> None:
        same = unicodedata.normalize("NFC", first) == unicodedata.normalize("NFC", second)

        assert (derived_id(SHA256, v=first) == derived_id(SHA256, v=second)) is same

    @given(left=st.text(max_size=8), right=st.text(max_size=8))
    def test_the_boundary_between_values_is_part_of_the_key(self, left: str, right: str) -> None:
        joined = derived_id(SHA256, a=left + right, b="")
        split = derived_id(SHA256, a=left, b=right)

        assert (joined == split) is (right == "")


def _equal_copy(value: object) -> object:
    match value:
        case Decimal():
            return Decimal(str(value)) * 1
        case datetime():
            return value.astimezone(UTC)
        case str():
            return unicodedata.normalize("NFD", value)
        case _:
            return value
