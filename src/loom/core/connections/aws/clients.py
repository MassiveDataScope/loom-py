"""The AWS clients of one process: built once per distinct setting, shared, closed together."""

from __future__ import annotations

from collections.abc import Hashable
from threading import Lock
from typing import Any

from loom.core.connections.aws.config import AwsClientConfig
from loom.core.connections.aws.loader import aws_island


class AwsClients:
    """Every boto3 client a process uses, one per service and distinct client settings.

    Args:
        config: The settings of a client asked for with no settings of its own.
    """

    def __init__(self, config: AwsClientConfig) -> None:
        self._config = config
        self._lock = Lock()
        self._clients: dict[Hashable, Any] = {}
        self._sessions: dict[str | None, Any] = {}

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
        """Release the connections of every client built so far.

        The clients stay cached and usable: each reconnects on its next call, and a
        later ``close`` releases it again.

        Raises:
            ExceptionGroup: When one or more clients failed to close, after every
                other client was closed.
        """
        failures: list[Exception] = []
        for client in tuple(self._clients.values()):
            try:
                client.close()
            except Exception as failure:
                failures.append(failure)
        if failures:
            raise ExceptionGroup("AWS clients failed to close", failures)

    def _build(self, service: str, config: AwsClientConfig) -> Any:
        island = aws_island()
        if config.profile not in self._sessions:
            self._sessions[config.profile] = island.open_session(config.profile)
        return island.build_aws_client(self._sessions[config.profile], service, config)


__all__ = ["AwsClients"]
