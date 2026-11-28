"""Multi-role HTTP transport for the ClickHouse driver boundary (spec §2/§3).

ClickHouse only accepts several roles as REPEATED HTTP parameters
(``role=a&role=b``); a comma-joined value is parsed as a single role name and
rejected with code 511. ``clickhouse-connect`` 0.15.1 builds the request URL
with ``urlencode(final_params)`` — without ``doseq=True`` — so a sequence value
would travel as its ``repr``. ``loom.stores.clickhouse._client`` fixes that at
the driver boundary, only when the registry asks for it; these tests pin the
patch point so a future driver version can never degrade the workaround
silently.
"""

from __future__ import annotations

import inspect
import subprocess
import sys
from urllib.parse import urlencode

import pytest
from clickhouse_connect.driver import httpclient

from loom.core.config.errors import ConfigError
from loom.stores.clickhouse import ClickHouseConnectionRegistry
from loom.stores.clickhouse._client import (
    enable_repeated_query_params,
    supports_repeated_query_params,
)
from tests.unit.core.sql._fakes import (
    RecordingClientFactory,
    make_connection_config,
    make_sql_config,
)

_URLENCODE = "urlencode"
_ROLE_A = "role_a"
_ROLE_B = "role_b"
_NO_REPEATED_PARAMS = "one 'role' HTTP parameter per role"

_IMPORT_ONLY_SCRIPT = """
from urllib.parse import urlencode

from clickhouse_connect.driver import httpclient

import loom.stores.clickhouse._client  # noqa: F401

params = {"role": ("a", "b")}
print("stock" if httpclient.urlencode(params) == urlencode(params) else "patched")
"""


@pytest.fixture
def patched_encoder(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enable the workaround for one test; ``monkeypatch`` restores the driver afterwards."""
    monkeypatch.setattr(httpclient, _URLENCODE, getattr(httpclient, _URLENCODE))
    enable_repeated_query_params()


def _multi_role_registry() -> ClickHouseConnectionRegistry:
    config = make_sql_config(
        analytics=make_connection_config(allowed_roles=(_ROLE_A, _ROLE_B), default_role=_ROLE_A)
    )
    return ClickHouseConnectionRegistry(config=config, client_factory=RecordingClientFactory())


class TestClickHouseTransport:
    def test_the_patched_driver_symbol_still_exists(self) -> None:
        """Loud failure if the driver stops exposing the encoder we rebind."""
        assert callable(getattr(httpclient, _URLENCODE, None))

    def test_the_driver_still_builds_its_url_with_the_patched_encoder(self) -> None:
        """Loud failure if the driver stops routing URL building through ``urlencode``."""
        source = inspect.getsource(httpclient.HttpClient._raw_request)
        assert "urlencode(final_params)" in source

    def test_importing_the_boundary_does_not_patch_the_driver(self) -> None:
        """No import-time side effect: an unrelated consumer keeps the stock encoder.

        Checked in a fresh interpreter on purpose: once any test in this session
        starts a multi-role registry the workaround is enabled process-wide, which
        is exactly why it must never happen behind a plain import.
        """
        completed = subprocess.run(  # noqa: S603 - fixed argv, no shell, no user input
            [sys.executable, "-c", _IMPORT_ONLY_SCRIPT],
            capture_output=True,
            text=True,
            check=False,
        )
        assert (completed.returncode, completed.stdout.strip()) == (0, "stock")

    def test_sequence_settings_become_repeated_http_parameters(self, patched_encoder: None) -> None:
        """A tuple of roles is what ClickHouse needs: one parameter per role."""
        assert (
            httpclient.urlencode({"role": (_ROLE_A, _ROLE_B)}) == f"role={_ROLE_A}&role={_ROLE_B}"
        )

    def test_scalar_settings_encode_exactly_like_the_stock_encoder(
        self, patched_encoder: None
    ) -> None:
        """The workaround is backward compatible: scalars are untouched."""
        params = {"role": _ROLE_A, "limit": 4, "readonly": 1, "query": "SELECT 1", "flag": True}
        assert httpclient.urlencode(params) == urlencode(params)

    def test_enabling_the_workaround_is_idempotent(self, patched_encoder: None) -> None:
        """Startup may run it twice (two multi-role connections) without stacking."""
        enable_repeated_query_params()
        assert (
            httpclient.urlencode({"role": (_ROLE_A, _ROLE_B)}) == f"role={_ROLE_A}&role={_ROLE_B}"
        )

    def test_capability_probe_reports_repeated_parameter_support(
        self, patched_encoder: None
    ) -> None:
        """The probe answers on the encoder the driver will actually use."""
        assert supports_repeated_query_params() is True

    def test_capability_probe_is_false_when_the_driver_drops_the_seam(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A driver that stops exposing the encoder must not report support."""
        monkeypatch.delattr(httpclient, _URLENCODE)
        assert supports_repeated_query_params() is False

    async def test_startup_fails_closed_when_the_driver_drops_the_seam(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A multi-role allowlist without repeated-parameter support aborts startup."""
        monkeypatch.delattr(httpclient, _URLENCODE)
        registry = _multi_role_registry()
        with pytest.raises(ConfigError, match=_NO_REPEATED_PARAMS):
            async with registry:
                pass

    async def test_a_multi_role_connection_enables_the_workaround_at_startup(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The registry is the only place that mutates the driver, and only when needed."""
        monkeypatch.setattr(httpclient, _URLENCODE, urlencode)
        async with _multi_role_registry():
            assert supports_repeated_query_params() is True

    async def test_single_role_connections_do_not_enable_the_workaround(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One role travels as a plain scalar, so single-role setups never need the shim."""
        monkeypatch.setattr(httpclient, _URLENCODE, urlencode)
        config = make_sql_config(
            analytics=make_connection_config(allowed_roles=(_ROLE_A,), default_role=_ROLE_A)
        )
        async with ClickHouseConnectionRegistry(
            config=config, client_factory=RecordingClientFactory()
        ) as registry:
            assert (registry.executor("analytics") is not None, httpclient.urlencode) == (
                True,
                urlencode,
            )
