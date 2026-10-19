"""REST-layer constants: query parameter names and profile defaults."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType


class QueryParam(StrEnum):
    """Public query parameter names for REST list/filter/pagination routes.

    All values are plain strings at runtime (``StrEnum``), so they can be
    passed directly to any API expecting ``str`` — e.g. ``request.query_params.get``.

    Example::

        page = int(query_params.get(QueryParam.PAGE, 1))
    """

    PAGE = "page"
    LIMIT = "limit"
    PAGINATION = "pagination"
    AFTER = "after"
    CURSOR = "cursor"
    SORT = "sort"
    DIRECTION = "direction"
    PROFILE = "profile"


PROFILE_DEFAULT = "default"

LIMIT_DEFAULT = 50
"""Rows per page when the request brings no ``?limit=``, capped by the route maximum."""

WWW_AUTHENTICATE_HEADER = "WWW-Authenticate"
"""Header carrying the authentication challenge of a ``401`` response."""

BEARER_CHALLENGE: Mapping[str, str] = MappingProxyType({WWW_AUTHENTICATE_HEADER: "Bearer"})
"""The single challenge the framework issues, shared by every 401 producer."""
