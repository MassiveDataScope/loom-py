"""The AWS clients of one process: built once per distinct setting, shared, closed together."""

from __future__ import annotations

from collections.abc import Hashable
from threading import Lock
from typing import Any

from loom.core.connections.aws.config import AwsClientConfig
from loom.core.connections.aws.loader import aws_island


class AwsClients:
    """Every boto3 client a process uses, one per service and distinct client settings.

    A client is thread-safe once built, so a lookup of one already built takes
    no lock. Only the first creation of a client, and of the session it comes
    from, runs under the lock, because a session must not be shared across
    threads while clients are built from it. The sessions are one per
    credentials profile: a single one unless a client names another profile.

    Args:
        config: The settings of a client asked for with no settings of its own.
    """

    def __init__(self, config: AwsClientConfig) -> None:
        self._config = config
        self._lock = Lock()
        self._clients: dict[Hashable, Any] = {}
        self._sessions: dict[str | None, Any] = {}

    @property
    def open_clients(self) -> int:
        """How many clients are built and not yet closed."""
        return len(self._clients)

    def client(self, service: str, config: AwsClientConfig | None = None) -> Any:
        """Return the client of *service* for *config*, built on the first request.

        Args:
            service: The boto3 service name.
            config: Settings of this client; ``None`` takes the ones this object was given.

        Returns:
            The client; typed ``Any`` because boto3 ships no stubs.

        Raises:
            MissingExtraError: When boto3 is not installed, naming the ``aws`` extra.
        """
        settings = self._config if config is None else config
        key = (service, settings.client_key())
        built = self._clients.get(key)
        if built is not None:
            return built
        with self._lock:
            built = self._clients.get(key)
            if built is None:
                built = self._clients[key] = self._build(service, settings)
            return built

    def close(self) -> None:
        """Close every client built so far and forget it, and the sessions with them."""
        with self._lock:
            clients = tuple(self._clients.values())
            self._clients.clear()
            self._sessions.clear()
        for client in clients:
            client.close()

    def _build(self, service: str, config: AwsClientConfig) -> Any:
        island = aws_island()
        if config.profile not in self._sessions:
            self._sessions[config.profile] = island.open_session(config.profile)
        return island.build_aws_client(self._sessions[config.profile], service, config)


__all__ = ["AwsClients"]
