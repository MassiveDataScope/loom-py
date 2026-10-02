"""Binary and network-address columns round-trip through the repository under row-level security."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.core.backend.sqlalchemy import compile_all, reset_registry
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.rls import create_schema, rls_session_settings
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from tests.integration.agnosticism import devices
from tests.integration.rls.conftest import BootstrapFactory, application_for, scalar

pytestmark = pytest.mark.integration

OWNER = "11111111-1111-1111-1111-111111111111"
OTHER = "22222222-2222-2222-2222-222222222222"
FINGERPRINT = b"\x00\xffloom\x80"


async def test_bytes_and_inet_values_are_written_and_read_back_by_their_owner_only(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database(devices.SCHEMA)
    application = application_for(devices, database, tmp_path)
    await create_schema(database.migrator, application)
    reset_registry()
    compile_all(*devices.MODELS)
    manager = SessionManager(database.write, session_settings=rls_session_settings(application))
    repository: RepositorySQLAlchemy[devices.Device, int] = RepositorySQLAlchemy(
        session_manager=manager, model=devices.Device
    )
    identity = set_identity(Identity(subject=OWNER))
    try:
        host = await repository.create(
            devices.RegisterDevice(owner_id=OWNER, fingerprint=FINGERPRINT, address="10.1.2.3")
        )
        network = await repository.create(
            devices.RegisterDevice(owner_id=OWNER, fingerprint=b"", address="2001:db8::1/64")
        )
        read_host = await repository.get_by_id(host.id)
        read_network = await repository.get_by_id(network.id)
    finally:
        reset_identity(identity)
    other = set_identity(Identity(subject=OTHER))
    try:
        hidden = await repository.get_by_id(host.id)
    finally:
        reset_identity(other)
        await manager.dispose()
        reset_registry()

    assert (host.fingerprint, host.address) == (FINGERPRINT, "10.1.2.3")
    assert read_host is not None
    assert (read_host.fingerprint, read_host.address) == (FINGERPRINT, "10.1.2.3")
    assert read_network is not None
    assert read_network.address == "2001:db8::1/64"
    assert hidden is None
    stored = "SELECT encode(fingerprint, 'hex') || ' ' || host(address) FROM devices.devices"
    assert (
        await scalar(database.bypass, f"{stored} WHERE id = {host.id}") == "00ff6c6f6f6d80 10.1.2.3"
    )
