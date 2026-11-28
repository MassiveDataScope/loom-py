"""A package scan hands its classes over; the REST side recognises its interfaces among them."""

from __future__ import annotations

from loom.core.discovery.scanner import PackageScanner
from loom.rest.discovery import interfaces_in, route_use_cases
from tests.helpers.package_writer import PackageWriter

_ROUTES = """\
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class AcmePing(UseCase[object, str]):
    async def execute(self) -> str:
        return "pong"


class AcmePingInterface(RestInterface[str]):
    prefix = "/ping"
    routes = (RestRoute(use_case=AcmePing, method="GET", path="/"),)
"""


class TestRestComponentsOfAScan:
    def test_the_scan_finds_the_interfaces_but_does_not_collect_their_use_cases(
        self, write_package: PackageWriter
    ) -> None:
        write_package("acme_rest_scan", {"routes.py": _ROUTES})

        discovered = PackageScanner(["acme_rest_scan"]).discover()

        assert [cls.__name__ for cls in interfaces_in(discovered)] == ["AcmePingInterface"]
        assert [cls.__name__ for cls in discovered.use_cases] == ["AcmePing"]

    def test_route_use_cases_lists_each_use_case_once_in_route_order(
        self, write_package: PackageWriter
    ) -> None:
        write_package("acme_rest_routes", {"routes.py": _ROUTES})
        discovered = PackageScanner(["acme_rest_routes"]).discover()
        (interface,) = interfaces_in(discovered)

        assert route_use_cases([interface, interface]) == (interface.routes[0].use_case,)

    def test_a_scan_of_no_interface_gives_none(self, write_package: PackageWriter) -> None:
        write_package("acme_no_rest", {"plain.py": "class Plain:\n    pass\n"})

        assert interfaces_in(PackageScanner(["acme_no_rest"]).discover()) == ()
