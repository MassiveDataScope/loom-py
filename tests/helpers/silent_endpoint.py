"""An HTTP endpoint that accepts connections and never answers, for client-bound tests."""

from __future__ import annotations

import socket
import threading
from collections.abc import Iterator

import pytest


@pytest.fixture
def silent_endpoint() -> Iterator[str]:
    """Serve ``http://127.0.0.1:<port>``: connections are accepted and never answered."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(16)
    held: list[socket.socket] = []
    stop = threading.Event()

    def _accept() -> None:
        server.settimeout(0.1)
        while not stop.is_set():
            try:
                held.append(server.accept()[0])
            except TimeoutError:
                continue

    thread = threading.Thread(target=_accept, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.getsockname()[1]}"
    finally:
        stop.set()
        thread.join()
        for connection in held:
            connection.close()
        server.close()
