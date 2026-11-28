"""The REST components of a package scan: interfaces and the use cases their routes reference."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from loom.core.discovery.base import DiscoveryResult
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface


def interfaces_in(discovered: DiscoveryResult) -> tuple[type[RestInterface[Any]], ...]:
    """The ``RestInterface`` subclasses the scan found, in scan order."""
    return tuple(
        cls
        for cls in discovered.classes
        if issubclass(cls, RestInterface) and cls is not RestInterface
    )


def route_use_cases(
    interfaces: Iterable[type[RestInterface[Any]]],
) -> tuple[type[UseCase[Any, Any]], ...]:
    """The unique use cases the routes of *interfaces* reference, first seen first.

    Generated CRUD use cases live in ``loom.rest.autocrud``, not in a scanned
    module, so only the routes lead to them.
    """
    return tuple(
        dict.fromkeys(route.use_case for interface in interfaces for route in interface.routes)
    )
