"""Who emits the records: the service and the kind of process."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType

from loom.core.environment import Environment
from loom.core.log_names import LogField


class LogRuntime(StrEnum):
    """The kind of process that emits the records."""

    FASTAPI = "fastapi"
    CELERY = "celery"
    LAMBDA = "lambda"
    CLI = "cli"


@dataclass(slots=True)
class LogIdentity:
    """Which service, and which process of it, emits the records.

    The runtime is known only once an entry point composes the application, after the
    configuration that builds this identity has loaded, so it is the one mutable part:
    every output holds this same instance, and :meth:`run_as` changes what the
    following records say on all of them.

    Attributes:
        environment: The environment the process runs in.
        runtime: The kind of process.
        service_name: ``app.name``; absent for a process with no application.
        service_version: ``app.version`` when declared.
    """

    environment: Environment
    runtime: LogRuntime = LogRuntime.CLI
    service_name: str | None = None
    service_version: str | None = None
    _fields: Mapping[str, str] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._fields = self._known()

    def run_as(self, runtime: LogRuntime) -> None:
        """Name *runtime* as the kind of process from now on."""
        self.runtime = runtime
        self._fields = self._known()

    def fields(self) -> Mapping[str, str]:
        """The fields it writes, without those it does not know."""
        return self._fields

    def _known(self) -> Mapping[str, str]:
        known = {
            LogField.SERVICE_NAME: self.service_name,
            LogField.SERVICE_VERSION: self.service_version,
            LogField.ENVIRONMENT: self.environment.value,
            LogField.RUNTIME: self.runtime.value,
        }
        return MappingProxyType({key: value for key, value in known.items() if value is not None})
