from __future__ import annotations

import pytest

from loom.core.use_case import Created


@pytest.mark.parametrize(
    "location",
    ["", "/items/1\r\nSet-Cookie: a=b", "/items/1 2", "/ítems/1", "/items/\t1"],
    ids=["empty", "crlf", "space", "non-ascii", "tab"],
)
def test_a_location_a_header_cannot_carry_is_refused(location: str) -> None:
    with pytest.raises(ValueError, match="Created.location"):
        Created("item", location=location)
