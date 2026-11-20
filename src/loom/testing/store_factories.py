"""Registered factories for the store value objects whose every field has a meaningful default.

Importing ``make`` or ``SampleFactory`` from :mod:`loom.testing` registers them, so
``make(QuerySpec, limit=2)`` keeps the offset mode, the first page and no filter
instead of a generated one, and any type nesting these values builds with no
declaration.
"""

from __future__ import annotations

from loom.core.stores.repository.query import FilterParams, PageParams, QuerySpec
from loom.testing.data import SampleFactory


class PageParamsFactory(SampleFactory[PageParams]):
    """The first page of the default size."""

    __set_as_default_factory_for_type__ = True
    __use_defaults__ = True


class FilterParamsFactory(SampleFactory[FilterParams]):
    """No equality filter."""

    __set_as_default_factory_for_type__ = True
    __use_defaults__ = True


class QuerySpecFactory(SampleFactory[QuerySpec]):
    """An unfiltered, unsorted query for the first offset page."""

    __set_as_default_factory_for_type__ = True
    __use_defaults__ = True
