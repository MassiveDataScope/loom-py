"""Who emits the records: the service, its environment and the kind of process."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from enum import StrEnum
from types import MappingProxyType

from loom.core.logger.vocabulary import LogField


class LogRuntime(StrEnum):
    """The kind of process that emits the records."""

    FASTAPI = "fastapi"
    CELERY = "celery"
    LAMBDA = "lambda"
    CLI = "cli"


@dataclass(frozen=True, slots=True)
class LogIdentity:
    """Which service, and which process of it, emits the records.

    Attributes:
        environment: The environment the process runs in.
        runtime: The kind of process.
        service_name: ``app.name``; absent for a process with no application.
        service_version: ``app.version`` when declared.
    """

    environment: str
    runtime: LogRuntime = LogRuntime.CLI
    service_name: str | None = None
    service_version: str | None = None

    def with_runtime(self, runtime: LogRuntime) -> LogIdentity:
        """This identity, running as *runtime*."""
        return replace(self, runtime=runtime)

    def fields(self) -> Mapping[str, str]:
        """The fields it stamps, without those it does not know; read-only, shared by records."""
        known = {
            LogField.SERVICE_NAME.value: self.service_name,
            LogField.SERVICE_VERSION.value: self.service_version,
            LogField.ENVIRONMENT.value: self.environment,
            LogField.RUNTIME.value: self.runtime.value,
        }
        return MappingProxyType({key: value for key, value in known.items() if value is not None})
