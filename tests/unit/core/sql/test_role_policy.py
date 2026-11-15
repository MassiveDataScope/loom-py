"""Fail-closed role policy tests for ``SqlQueryService`` (spec §3/§7.2)."""

from __future__ import annotations

import pytest

from loom.core.errors import Forbidden, NotFound
from loom.core.sql.abc import (
    RoleNotAllowedError,
    RoleRequiredError,
    UnknownConnectionError,
)
from tests.unit.core.sql._fakes import FakeSqlExecutor, make_connection_config
from tests.unit.core.sql.conftest import ServiceFactory


class TestSqlRolePolicy:
    async def test_rejects_external_role_when_there_is_no_allowlist(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """Without a configured allowlist, every caller-provided role is rejected."""
        service = make_service(
            fake_executor, make_connection_config(allowed_roles=(), default_role=None)
        )
        with pytest.raises(RoleNotAllowedError):
            await service.execute("SELECT 1", connection="analytics", roles=["role_viz_reader"])

    async def test_rejects_role_when_outside_the_allowlist(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """A role not present in ``allowed_roles`` produces RoleNotAllowedError."""
        service = make_service(fake_executor, make_connection_config())
        with pytest.raises(RoleNotAllowedError):
            await service.execute("SELECT 1", connection="analytics", roles=["role_intruder"])

    async def test_accepts_role_when_it_belongs_to_the_allowlist(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """An allowlisted role travels untouched in the executor options."""
        service = make_service(fake_executor, make_connection_config())
        await service.execute("SELECT 1", connection="analytics", roles=["role_viz_sales"])
        assert fake_executor.calls[0].options.roles == ("role_viz_sales",)

    async def test_uses_default_role_when_the_request_brings_no_role(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """Without a role in the request the connection ``default_role`` applies."""
        service = make_service(fake_executor, make_connection_config())
        await service.execute("SELECT 1", connection="analytics")
        assert fake_executor.calls[0].options.roles == ("role_viz_reader",)

    async def test_fails_role_required_when_no_role_and_no_default(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """Without an effective role, never run with the connection user's full roles."""
        service = make_service(fake_executor, make_connection_config(default_role=None))
        with pytest.raises(RoleRequiredError):
            await service.execute("SELECT 1", connection="analytics")

    async def test_does_not_execute_query_when_role_is_rejected(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """Role rejection happens before touching the executor (fail-closed)."""
        service = make_service(fake_executor, make_connection_config())
        with pytest.raises(RoleNotAllowedError):
            await service.execute("SELECT 1", connection="analytics", roles=["role_intruder"])
        assert fake_executor.calls == []

    async def test_does_not_execute_query_when_effective_role_is_missing(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """RoleRequiredError also cuts execution before reaching the executor."""
        service = make_service(fake_executor, make_connection_config(default_role=None))
        with pytest.raises(RoleRequiredError):
            await service.execute("SELECT 1", connection="analytics")
        assert fake_executor.calls == []

    async def test_connection_settings_do_not_override_the_policy_role(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """A ``settings.role`` on the connection never replaces the policy role."""
        service = make_service(
            fake_executor, make_connection_config(settings={"role": "role_admin_evil"})
        )
        await service.execute("SELECT 1", connection="analytics")
        assert fake_executor.calls[0].options.roles == ("role_viz_reader",)

    async def test_accepts_several_allowlisted_roles_in_one_query(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """A query may carry several roles: the backend applies their union."""
        service = make_service(fake_executor, make_connection_config())
        await service.execute(
            "SELECT 1", connection="analytics", roles=["role_viz_reader", "role_viz_sales"]
        )
        assert fake_executor.calls[0].options.roles == ("role_viz_reader", "role_viz_sales")

    async def test_rejects_the_whole_request_when_one_role_is_outside_the_allowlist(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """The allowlist is validated per element and never silently filters."""
        service = make_service(fake_executor, make_connection_config())
        with pytest.raises(RoleNotAllowedError):
            await service.execute(
                "SELECT 1", connection="analytics", roles=["role_viz_reader", "role_intruder"]
            )
        assert fake_executor.calls == []

    async def test_empty_roles_fall_back_to_the_default_role(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """An empty role sequence is 'no role requested', never 'all roles'."""
        service = make_service(fake_executor, make_connection_config())
        await service.execute("SELECT 1", connection="analytics", roles=[])
        assert fake_executor.calls[0].options.roles == ("role_viz_reader",)

    async def test_unknown_connection_raises_unknown_connection_error(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """An unconfigured connection produces UnknownConnectionError, not KeyError."""
        service = make_service(fake_executor, make_connection_config())
        with pytest.raises(UnknownConnectionError):
            await service.execute("SELECT 1", connection="nonexistent")

    def test_role_errors_inherit_from_forbidden(self) -> None:
        """Both role errors reuse Forbidden to map to 403 without touching the API."""
        assert (
            issubclass(RoleNotAllowedError, Forbidden),
            issubclass(RoleRequiredError, Forbidden),
        ) == (True, True)

    def test_unknown_connection_inherits_from_not_found(self) -> None:
        """UnknownConnectionError reuses NotFound to map to 404 without touching the API."""
        assert issubclass(UnknownConnectionError, NotFound)
