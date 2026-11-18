"""The suite reaches loopback and nothing else: pytest-socket refuses any other host."""

from __future__ import annotations

import socket
import tempfile
from pathlib import Path

import pytest
from pytest_socket import SocketConnectBlockedError


class TestNetworkGuard:
    """Refuse every non-loopback ``connect``; ``connect_ex`` and DNS lookups are not guarded."""

    def test_a_connection_to_a_remote_host_is_refused(self) -> None:
        with pytest.raises(SocketConnectBlockedError):
            socket.create_connection(("203.0.113.9", 9), timeout=1)

    def test_a_connection_to_loopback_is_allowed(self) -> None:
        with socket.create_server(("127.0.0.1", 0)) as server:
            port = server.getsockname()[1]
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                pass

    def test_a_unix_socket_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            path = str(Path(directory) / "s")
            with socket.socket(socket.AF_UNIX) as server:
                server.bind(path)
                server.listen()
                with socket.socket(socket.AF_UNIX) as client:
                    client.connect(path)
