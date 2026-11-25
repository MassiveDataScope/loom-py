"""Who emits the records: the service and the kind of process."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

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

    Every output loom installs holds the same instance, so :meth:`run_as` changes
    what the following records say on all of them.

    Attributes:
        service_name: ``app.name``; absent for a process with no application.
        service_version: ``app.version`` when declared.
        environment: The environment the process runs in.
        runtime: The kind of process.
    """

    environment: Environment
    runtime: LogRuntime = LogRuntime.CLI
    service_name: str | None = None
    service_version: str | None = None

    def run_as(self, runtime: LogRuntime) -> None:
        """Name *runtime* as the kind of process from now on."""
        self.runtime = runtime

    def fields(self) -> dict[str, str]:
        """The fields it writes, without those it does not know."""
        known = {
            LogField.SERVICE_NAME: self.service_name,
            LogField.SERVICE_VERSION: self.service_version,
            LogField.ENVIRONMENT: self.environment.value,
            LogField.RUNTIME: self.runtime.value,
        }
        return {key: value for key, value in known.items() if value is not None}
