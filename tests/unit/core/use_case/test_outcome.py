from __future__ import annotations

from typing import Any

import pytest

from loom.core.use_case import Created


def test_an_absolute_uri_is_a_location() -> None:
    location = "https://api.example.com/v1/items/1?view=full#top"

    assert Created("item", location=location).location == location


@pytest.mark.parametrize(
    "location",
    ["", "/items/1\r\nSet-Cookie: a=b", "/items/1 2", "/ítems/1", "/items/%zz"],
    ids=["empty", "crlf", "space", "non-ascii", "bad-percent"],
)
def test_a_location_that_is_not_a_uri_reference_is_refused(location: Any) -> None:
    with pytest.raises(ValueError, match="Created.location"):
        Created("item", location=location)
